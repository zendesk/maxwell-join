package com.zendesk.maxwelljoin

import java.util.Properties
import com.zendesk.maxwelljoin.mawxwelljoin.{MaxwellData, MapByID}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams._

import scala.collection.immutable.HashMap

package object mawxwelljoin {
  type MaxwellData = Map[String, Any]
  type MapByID = Map[BigInt, MaxwellData]
}

case class MaxwellKey(val table: String, val database: String, val pk: List[Any])
case class MaxwellTableDatabase(val table: String, val database: String)
case class MaxwellValue(val rowType: String, val database: String, val table: String,
                        val ts: BigInt, val xid: BigInt,
                        val data: Map[String, Any])

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


case class TableFilter(table: String) extends Predicate[MaxwellKey, MaxwellValue] {
  override def test(key: MaxwellKey, value: MaxwellValue): Boolean = key.table == table
}

case class MapMaxwellValue() extends ValueMapper[MaxwellValue, MaxwellData] {
  override def apply(value: MaxwellValue) = value.data
}


object MaxwellJoin extends App {

  implicit val formats = org.json4s.DefaultFormats

  val builder: KStreamBuilder = new KStreamBuilder

  val streamingConfig = {
    val settings = new Properties
    settings.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    settings.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    settings.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    settings.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    settings.put(StreamsConfig.JOB_ID_CONFIG, "maxwell-joiner")
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "docker-vm:9092")
    settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "docker-vm:2181/kafka")
    settings
  }

  val maxwellInputStream: KStream[MaxwellKey, MaxwellValue] =
    builder.stream(MaxwellKeyDeserializer(), JsonDeserializer[MaxwellValue](), "maxwell")


  class TicketFieldAggregator extends Aggregator[MaxwellKey, MapByID, MapByID] {
    override def apply(aggKey: MaxwellKey, value: MapByID, aggregate: MapByID): MapByID = {
      aggregate ++ value
    }
  }

  val aggregateStream = maxwellInputStream
    .filter(TableFilter("ticket_field_entries"))
    .map(new KeyValueMapper[MaxwellKey, MaxwellValue, KeyValue[MaxwellKey, MapByID]] {
      override def apply(key: MaxwellKey, value: MaxwellValue) = {
        val document = value.data
        val map : MapByID = document.get("id") match {
          case Some(intID : BigInt) => Map(intID -> document)
          case _ => Map[BigInt, MaxwellData]()
        }
        val newKey = document.get("ticket_id") match {
          case Some(ticketID : BigInt) =>
            MaxwellKey("tickets", key.database, List(ticketID))
        }

        new KeyValue(newKey, map)
      }
    })
    .aggregateByKey(
      new Initializer[MapByID]() {
        override def apply() = Map[BigInt, MaxwellData]()
      },
      new TicketFieldAggregator(),
      RowKeySerializer(),
      JsonSerializer[MapByID](),
      RowKeyDeserializer(),
      JsonDeserializer[MapByID](),
      "fields-by-ticket"
    )

  val tfeAggregateTable =
    aggregateStream.through("maxwell-join-ticket_field_entries-by-ticket",
      RowKeySerializer(), JsonSerializer[MapByID](),
      RowKeyDeserializer(), JsonDeserializer[MapByID]()
    )

  val ticketsTable: KTable[MaxwellKey, MaxwellData] =
    maxwellInputStream
      .filter(TableFilter("tickets"))
      .mapValues(MapMaxwellValue())
      .reduceByKey( new Reducer[MaxwellData]() {
        override def apply(v1: MaxwellData, v2: MaxwellData) = v2
        },
        RowKeySerializer(), JsonSerializer[MaxwellData](), MaxwellKeyDeserializer(), JsonDeserializer[MaxwellData](),
        "reduce-tickets-table"
      ).through("maxwell-join-tickets", RowKeySerializer(), JsonSerializer[MaxwellData], RowKeyDeserializer(), JsonDeserializer[MaxwellData]())

  ticketsTable.join(tfeAggregateTable, new ValueJoiner[MaxwellData, MapByID, MaxwellData] {
    override def apply(value1: MaxwellData, value2: MapByID): MaxwellData = {
      value1 + ("ticket_field_entries" -> value2.values.toList)
    }
  }).to("maxwell-join-final-tickets", RowKeySerializer(), JsonSerializer[MaxwellData])

  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object KeyValueImplicits {
  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)
}
