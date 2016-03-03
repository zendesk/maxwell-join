package com.zendesk.maxwelljoin

import java.util.{Properties, Date}

import akka.routing.{RoundRobinPool, RoundRobinRoutingLogic, Router}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import akka.actor.{ActorRef, Props, ActorSystem, Actor}
import org.apache.kafka.common.TopicPartition
import org.json4s.FieldSerializer._

import org.json4s._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class RawMessage(record: ConsumerRecord[String, String])
case class Processed(partition: Int, offset: Long)
case class MaxwellRow(rowType: String, database: String, table: String, data: Map[String, Any])

case class ParsedMessage(partition: Int, offset: Long, primaryKey: List[Any], row: MaxwellRow, topSender: ActorRef)


case class PartitionOffset(map: Map[Long, Boolean] = Map(), min: Long = Long.MaxValue ) {
  def start(offset: Long) =
    PartitionOffset(map + (offset -> false), Math.min(min, offset))

  def finish(offset: Long) = {
    val m = map + (offset -> true)

    var lastFinished = min
    val finishedOffsets = ListBuffer[Long]()

    while ( m.getOrElse(lastFinished, false) ) {
      finishedOffsets += lastFinished
      lastFinished = lastFinished + 1
    }

    println("received finish message for " + offset + " : " + finishedOffsets)
    finishedOffsets.lastOption.map { o =>
      println("could commit at #" + (o + 1))
    }

    val newMin = finishedOffsets.lastOption.map(_ + 1).getOrElse(min)
    // rely on the fact that we're guaranteed per-actor in-order delivery
    PartitionOffset(m -- finishedOffsets, newMin)
  }
}

class OffsetManager extends Actor {
  val parsers = context.actorOf(Props[Parser].withRouter(RoundRobinPool(5)), "json-parser")
  /* partition -> ( offset -> finished ) */
  var outstanding: Map[Int, PartitionOffset] = Map()
  var minOffsets: Map[Int, Long] = Map()

  private def updateOutstandingRows(partition: Int, offset: Long, complete: Boolean): Unit = {
    val partitionOffset = outstanding.getOrElse(partition, PartitionOffset())
    val updatedPartitionOffset = partitionOffset.start(offset)

    outstanding = outstanding + (partition -> updatedPartitionOffset)
  }

  def receive = {
    case m: RawMessage =>
      val partitionOffset = outstanding.getOrElse(m.record.partition, PartitionOffset())
      outstanding += (m.record.partition -> partitionOffset.start(m.record.offset))
      parsers ! m
    case m: Processed =>
      val partitionOffset = outstanding.getOrElse(m.partition, PartitionOffset())
      outstanding += (m.partition -> partitionOffset.finish(m.offset))
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
class MaxwellJoinKafkaConsumer(actor: ActorRef) {
  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test-consumer")
  props.put("enable.auto.commit", "false")
  props.put("session.timeout.ms", "30000")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  props.put("auto.offset.reset", "earliest")

  val consumer = new KafkaConsumer[String, String](props)
  def run = {
    consumer.subscribe(List("maxwell"))
    consumer.

    val topicPartition = new TopicPartition("maxwell", 0)

    while (true) {
      val records = consumer.poll(100)
      for (record <- records) {
        actor ! RawMessage(record)

        println(record.key())
      }
    }
  }
}
object MaxwellJoin extends App {
  val system = ActorSystem("maxwell-join")

  val props = Props[OffsetManager]
  val parsers = system.actorOf(Props[OffsetManager].withMailbox("bounded-mailbox"), "offset-manager")
  val consumer = new MaxwellJoinKafkaConsumer(parsers)
  consumer.run
}
