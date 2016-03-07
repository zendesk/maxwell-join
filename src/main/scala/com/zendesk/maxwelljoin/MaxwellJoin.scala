package com.zendesk.maxwelljoin

import java.util.concurrent.{TimeoutException, ConcurrentLinkedQueue, TimeUnit}
import scala.concurrent.duration._

import java.util.{Properties, Date}

import akka.routing.{RoundRobinPool, RoundRobinRoutingLogic, Router}
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, ConsumerRecord, KafkaConsumer}

import akka.actor._
import org.apache.kafka.common.TopicPartition
import org.json4s.FieldSerializer._

import org.json4s._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class GetCommitOffsets()
case class FlushCommitOffsets()
case class RawMessage(record: ConsumerRecord[String, String])
case class Processed(partition: Int, offset: Long)
case class CommitOffsets(offsets: Map[Int, Long])
case class MaxwellRow(rowType: String, database: String, table: String, data: Map[String, Any])

case class ParsedMessage(partition: Int, offset: Long, primaryKey: List[Any], row: MaxwellRow, topSender: ActorRef)


case class PartitionOffset(map: Map[Long, Boolean] = Map(), min: Long = Long.MaxValue ) {
  def start(offset: Long) =
    PartitionOffset(map + (offset -> false), Math.min(min, offset))

  def finish(offset: Long)(commit: (Long) => Unit) = {
    val m = map + (offset -> true)

    var lastFinished = min
    val finishedOffsets = ListBuffer[Long]()

    while ( m.getOrElse(lastFinished, false) ) {
      finishedOffsets += lastFinished
      lastFinished = lastFinished + 1
    }

    finishedOffsets.lastOption.map { o =>
      commit(o + 1)
    }

    val newMin = finishedOffsets.lastOption.map(_ + 1).getOrElse(min)
    // rely on the fact that we're guaranteed per-actor in-order delivery
    PartitionOffset(m -- finishedOffsets, newMin)
  }
}

class OffsetManager extends Actor {
  val parsers = context.actorOf(Props[Parser].withRouter(RoundRobinPool(5)), "json-parser")
  /* partition -> ( offset -> finished ) */
  var outstanding: Map[Int, PartitionOffset] = Map().withDefault { partition => PartitionOffset() }
  var minOffsets: Map[Int, Long] = Map()

  var commitOffsets = Map[Int, Long]()

  def receive = {
    case m: RawMessage =>
      val partitionOffset = outstanding(m.record.partition)
      outstanding += (m.record.partition -> partitionOffset.start(m.record.offset))
      parsers ! m
    case m: Processed =>
      val partitionOffset = outstanding(m.partition)
      val newOffsetMap = partitionOffset.finish(m.offset) { offset =>
        commitOffsets += (m.partition -> offset)
      }
      outstanding += (m.partition -> newOffsetMap)
    case m: GetCommitOffsets =>
      sender ! CommitOffsets(commitOffsets)
    case m: FlushCommitOffsets =>
      commitOffsets = Map()
  }
}

class Parser extends Actor {
  val renameMaxwellRowType = FieldSerializer[MaxwellRow](
    renameTo("rowType", "type"),
    renameFrom("type", "rowType")
  )
  implicit val formats = org.json4s.DefaultFormats + renameMaxwellRowType

  val filter = context.actorOf(Props[Filter])

  private def parsePrimaryKey(s: String): List[Any] = {
    val parsedKey = org.json4s.native.JsonMethods.parse(s)
    val map = parsedKey.extract[Map[String, Any]]

    map.filter {
      case (k, v) => k.startsWith("pk.")
    }.toList.sortWith(_._1 < _._1).map(_._2)
  }

  private def parse(raw: RawMessage) = {
    val record = raw.record

    val pkList = parsePrimaryKey(record.key)
    val parsedBody = org.json4s.native.JsonMethods.parse(record.value)
    val row = parsedBody.extract[MaxwellRow]
    println("parsing message #" + record.offset)
    filter ! ParsedMessage(record.partition, record.offset, pkList, row, sender)
  }

  def receive = {
    case m : RawMessage => parse(m)

  }
}

class Filter extends Actor {
  def receive = {
    case m : ParsedMessage =>
      Thread.sleep(Random.nextInt(10))
      if ( m.row.table == "tickets" ) {
        println("filter got ticket: " + m)
        m.topSender ! Processed(m.partition, m.offset)
      } else {
        println("filter got other: " + m)
        m.topSender ! Processed(m.partition, m.offset)
      }
  }
}
class MaxwellJoinKafkaConsumer(inbox: Inbox, actor: ActorRef) {
  val props = new Properties()

  props.put("bootstrap.servers", "docker-vm:9092")
  props.put("group.id", "test-consumer-2")
  props.put("enable.auto.commit", "false")
  props.put("session.timeout.ms", "30000")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](props)

  private def commitOffsets() = {
    println("checking offsets...")
    try {
      inbox.send(actor, GetCommitOffsets())
      inbox.receive(100 milliseconds) match {
        case m: CommitOffsets =>
          if (m.offsets.size > 0) {
            println("aaa Got CommitOffset: " + m)
            val kafkaCommitMap = m.offsets.map {
              case (partition, offset) =>
                val topicPartition = new TopicPartition("maxwell", partition)
                (topicPartition -> new OffsetAndMetadata(offset))
            }
            consumer.commitAsync(kafkaCommitMap, null)

            inbox.send(actor, FlushCommitOffsets())
          }
      }
    } catch {
      case x: TimeoutException => println("got timeout.")
    }
  }

  def run = {
    consumer.subscribe(List("maxwell"))
    println(consumer.assignment)
    consumer.seekToBeginning(consumer.assignment.toList : _*)

    while (true) {
      val records = consumer.poll(100)
      for (record <- records) {
        inbox.send(actor, RawMessage(record))
        println(record.key())
      }
      commitOffsets()
    }
  }
}
object MaxwellJoin extends App {
  val system = ActorSystem("maxwell-join")

  val props = Props[OffsetManager]
  val offsetManager = system.actorOf(Props[OffsetManager].withMailbox("bounded-mailbox"), "offset-manager")
  val consumer = new MaxwellJoinKafkaConsumer(Inbox.create(system), offsetManager)
  consumer.run
}
