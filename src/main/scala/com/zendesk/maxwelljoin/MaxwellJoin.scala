package com.zendesk.maxwelljoin

import java.util
import java.util.Properties
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams._
import org.apache.kafka.streams.processor.internals.WallclockTimestampExtractor
import org.json4s.JObject
import org.json4s.native.parseJson;
import org.json4s.native.Serialization.write;

case class MaxwellKey(val table: String, val database: String, val pk: List[Any])
case class MaxwellTableDatabase(val table: String, val database: String)

class ListAggregator extends Aggregator[MaxwellKey, String, List[String]] {
  override def apply(aggKey: MaxwellKey, value: String, aggregate: List[String]): List[String] = {
    aggregate :+ value
  }
}

// link ticket to user.  ok.  so we need to send ticket-user pairs (or all of them?)
// to the user channel... yes?

// so... two tables, one that aggregates


// KEY: TiCKET-1
// LEFT-VAL: TICKET
// RIGHT-VAL: LIST OF USERS

// users, aggregate to list of users on tikets key.
// that would join things, but it would also sprawl, wouldn't it?


class MaxwellKeyDeserializer extends Deserializer[MaxwellKey] {
  implicit val formats = org.json4s.DefaultFormats

  override def deserialize(topic: String, data: Array[Byte]): MaxwellKey = {
    val jvalue = parseJson(new String(data))
    jvalue match {
      case m : JObject =>
        val tblDB = m.extract[MaxwellTableDatabase]
        val pkList = m.values.filterKeys(_.startsWith("pk.")).toList.sortBy(_._1).map(_._2)
        MaxwellKey(tblDB.table, tblDB.database, pkList)
      case _ => throw new SerializationException("expected j-object as key")
    }
  }
  override def close(): Unit = {}
  override def configure(map: util.Map[String, _], b: Boolean) = {}
}

class MaxwellKeySerializer extends Serializer[MaxwellKey] {
  implicit val formats = org.json4s.DefaultFormats

  override def close(): Unit = {}
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}
  override def serialize(topic: String, t: MaxwellKey): Array[Byte] = write(t).getBytes
}

case class TableFilter(table: String) extends Predicate[MaxwellKey, String] {
  override def test(key: MaxwellKey, value: String): Boolean = key.table == table
}

abstract trait BasicDeserializer[T] extends Deserializer[T] {
  def close(): Unit = {}
  def configure(map: util.Map[String, _], b: Boolean): Unit = {}
}

class FieldMapDeseralizer extends BasicDeserializer[Map[BigInt, String]] {
  implicit val formats = org.json4s.DefaultFormats

  override def deserialize(s: String, bytes: Array[Byte]): Map[BigInt, String] = {
    if ( bytes == null )
      return null

    parseJson(new String(bytes)).extract[Map[BigInt, String]]
  }
}

class JsonSerializer[T <: AnyRef](implicit val manifest: Manifest[T]) extends Serializer[T] {
  implicit val formats = org.json4s.DefaultFormats

  override def close(): Unit = {}

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(topic: String, obj: T): Array[Byte] = {
    write[T](obj).getBytes
  }
}

object MaxwellJoin extends App {
  implicit val formats = org.json4s.DefaultFormats

  val builder: KStreamBuilder = new KStreamBuilder

  val streamingConfig = {
    val settings = new Properties
    settings.put(StreamsConfig.JOB_ID_CONFIG, "maxwell-joiner")
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "docker-vm:9092")
    settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "docker-vm:2181/kafka")

    settings.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    settings.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    settings.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    settings.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    settings
  }

  val maxwellInputStream: KStream[MaxwellKey, String] = builder.stream(new MaxwellKeyDeserializer(), new StringDeserializer(), "maxwell")

  val tickets = maxwellInputStream.filter(TableFilter("tickets"))

  class TicketFieldAggregator extends Aggregator[MaxwellKey, Map[BigInt, String], Map[BigInt, String]] {
    override def apply(aggKey: MaxwellKey, value: Map[BigInt, String], aggregate: Map[BigInt, String]): Map[BigInt, String] = {
      aggregate ++ value
    }
  }

  val tfeByTicketStream = maxwellInputStream
      .filter(TableFilter("ticket_field_entries"))
      .map(new KeyValueMapper[MaxwellKey, String, KeyValue[MaxwellKey, Map[BigInt, String]]] {
          override def apply(key: MaxwellKey, value: String) = {
            val document = (parseJson(value) \ "data").extract[Map[String, Any]]

            val map : Map[BigInt, String] = document.get("id") match {
                case Some(intID : BigInt) => Map(intID -> value)
                case _ => Map[BigInt, String]()
            }
            val newKey = document.get("ticket_id") match {
              case Some(ticketID : BigInt) =>
                MaxwellKey("tickets", key.database, List(ticketID))
            }

            new KeyValue(newKey, map)
          }
        })

    val tfeTable: KTable[MaxwellKey, Map[BigInt, String]] =
      tfeByTicketStream.aggregateByKey(
        new Initializer[Map[BigInt, String]]() {
          override def apply() = Map[BigInt, String]()
        },
        new TicketFieldAggregator(),
        new MaxwellKeySerializer(),
        new JsonSerializer[Map[BigInt, String]](),
        new MaxwellKeyDeserializer(),
        new FieldMapDeseralizer(),
        "fields-by-ticket"
      )

  tfeTable.to("maxwell-join-tfe", new MaxwellKeySerializer(), new JsonSerializer[Map[BigInt, String]]())

  val ticketsTable = tickets.through("maxwell-join-tickets",
    new MaxwellKeySerializer(), new StringSerializer(),
    new MaxwellKeyDeserializer(), new StringDeserializer())

  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object KeyValueImplicits {
  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)
}
