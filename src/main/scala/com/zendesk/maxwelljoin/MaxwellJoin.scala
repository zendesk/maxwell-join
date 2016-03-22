package com.zendesk.maxwelljoin

import java.util.Properties
import com.zendesk.maxwelljoin.mawxwelljoin.{MaxwellData, MapByID}
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.internals.{MaxwellKTable, KTableProcessorSupplier, KTableImpl}
import org.apache.kafka.streams.processor.ProcessorSupplier


package object mawxwelljoin {
  type MaxwellData = Map[String, Any]
  type MapByID = Map[BigInt, MaxwellData]
}

case class MaxwellKey(val database: String, val table: String, val pk: List[Map[String, Any]])
case class MaxwellTableDatabase(val table: String, val database: String)
case class MaxwellValue(val rowType: String, val database: String, val table: String,
                        val ts: BigInt, val xid: BigInt,
                        val data: Map[String, Any])


case class TableFilter(table: String) extends Predicate[MaxwellKey, MaxwellData] {
  override def test(key: MaxwellKey, value: MaxwellData): Boolean = key.table == table
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
    settings.put("partitioner.class", classOf[MaxwellJoinPartitioner])
    settings
  }

  val keySer      = MaxwellKeySerializer()
  val keyDeser    = MaxwellKeyDeserializer()
  val valSer      = JsonSerializer[MaxwellValue]()
  val valDeser    = JsonDeserializer[MaxwellValue]()
  val idMapSer    = JsonSerializer[MapByID]()
  val idMapDeser  = JsonDeserializer[MapByID]()
  val mxDataSer   = JsonSerializer[MaxwellData]()
  val mxDataDeser = JsonDeserializer[MaxwellData]()

  val maxwellInputTable: KTable[MaxwellKey, MaxwellData] = builder
    .table(keySer, valSer, keyDeser, valDeser, "maxwell")
    .mapValues(MapMaxwellValue())


  class TicketFieldAggregator extends Aggregator[MaxwellKey, MapByID, MapByID] {
    override def apply(aggKey: MaxwellKey, value: MapByID, aggregate: MapByID): MapByID = {
      aggregate ++ value
    }
  }


  class MaxwellJoinPartitioner extends DefaultPartitioner {
    override def partition(topic: String,
                           key: scala.AnyRef,
                           keyBytes: Array[Byte],
                           value: scala.AnyRef,
                           valueBytes: Array[Byte],
                           cluster: Cluster) = {
      topic match {
        case "maxwell-join-ticket_field_entries-by-ticket" =>
          val k = keyDeser.deserialize(topic, keyBytes)
          Math.abs(k.database.hashCode() % cluster.partitionCountForTopic(topic))
        case _ => super.partition(topic, key, keyBytes, value, valueBytes, cluster)
      }
    }
  }

  val aggregateStream = maxwellInputTable
    .toStream()
    .filter(TableFilter("ticket_field_entries"))
    .map(new KeyValueMapper[MaxwellKey, MaxwellData, KeyValue[MaxwellKey, MapByID]] {
      override def apply(key: MaxwellKey, value: MaxwellData) = {
        val document = value
        val map : MapByID = document.get("id") match {
          case Some(intID : BigInt) => Map(intID -> document)
          case _ => Map[BigInt, MaxwellData]()
        }
        val newKey = document.get("ticket_id") match {
          case Some(ticketID : BigInt) =>
            MaxwellKey(key.database, "tickets", List(Map("id" -> ticketID)))
        }

        new KeyValue(newKey, map)
      }
    })
    .aggregateByKey(
      new Initializer[MapByID]() {
        override def apply() = Map[BigInt, MaxwellData]()
      },
      new TicketFieldAggregator(),
      keySer, idMapSer, keyDeser, idMapDeser,
      "fields-by-ticket"
    )

  val tfeAggregateTable = aggregateStream.through(
    "maxwell-join-ticket_field_entries-by-ticket",
    keySer, idMapSer, keyDeser, idMapDeser
  )


  // start with addSource(...)
  // add key-remap processor
  // add join provider?  So you start with the raw stream


  // subclass the builder?  but then you have to output our table class
  // the whole damn thing has to be subclassed.

  // fork the fucking library?  seems nutso.

  // hack it and expose the names?

  val ticketsTable: KTable[MaxwellKey, MaxwellData] =
    maxwellInputTable.filter(TableFilter("tickets"))

  val mxKT = MaxwellKTable(ticketsTable)

  val tfeJoiner = new ValueJoiner[MaxwellData, MapByID, MaxwellData] {
    override def apply(value1: MaxwellData, value2: MapByID): MaxwellData = {
      if ( value2 != null )
        value1 + ("ticket_field_entries" -> value2.values.toList)
      else
        value1
    }
  }


  val userJoiner = new ValueJoiner[MaxwellData, MaxwellData, MaxwellData] {
    override def apply(value1: MaxwellData, value2: MaxwellData): MaxwellData = {
      if ( value2 != null)
        value1  + ("requester" -> value2)
      else
        value1
    }
  }
  val userMapper = new KeyMapper[MaxwellKey, MaxwellKey, MaxwellData] {
    override def apply(key: MaxwellKey, value: MaxwellData): MaxwellKey = {
      val userID = value.get("requester_id").get
      MaxwellKey(key.database, "users", List(Map("id" -> userID)))
    }
  }

  val userTable: KTable[MaxwellKey, MaxwellData] = maxwellInputTable.filter(TableFilter("users"))

  mxKT
    .join(userTable, userMapper, null, userJoiner)
    .join(tfeAggregateTable, tfeJoiner)
    .to("maxwell-join-final-tickets", keySer, mxDataSer)

  builder.stream(keyDeser, mxDataDeser, "maxwell-join-final-tickets").map(
    new KeyValueMapper[MaxwellKey, MaxwellData, KeyValue[MaxwellKey, MaxwellData]]() {
      override def apply(key: MaxwellKey, value: MaxwellData) = {
        println(s"k: $key, v: $value")
        new KeyValue(key, value)
      }
    }
  )

  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object KeyValueImplicits {
  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)
}
