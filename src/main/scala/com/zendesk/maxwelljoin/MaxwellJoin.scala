package com.zendesk.maxwelljoin


import java.util
import java.util.Properties
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.internals.KStreamImpl
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.state.Stores

import scala.collection.immutable.HashMap

case class MaxwellKey(val database: String, val table: String, val pk: List[Map[String, Any]])
case class MaxwellTableDatabase(val table: String, val database: String)
case class MaxwellValue(val rowType: String, val database: String, val table: String,
                        val ts: BigInt, val xid: BigInt,
                        val data: Map[String, Any])

case class MapMaxwellValue() extends ValueMapper[MaxwellValue, MaxwellData] {
  override def apply(value: MaxwellValue) = value.data
}

class BasicProcessorSupplier[K, V]( f: (ProcessorContext, K, V) => Unit ) extends ProcessorSupplier[K, V] {
  override def get(): Processor[K, V] = new BasicProcessor(f)
  class BasicProcessor[K, V](val f: (ProcessorContext, K, V) => Unit) extends AbstractProcessor[K, V] {
    override def process(key: K, value: V) = f(context, key, value)
  }
}

case class TableFilter(table: String) extends BasicProcessorSupplier[MaxwellKey, MaxwellData](
  (context, key, value) =>
    if ( key.table == table ) context.forward(key, value)
)




case class RootTableProcessor[K, V]()

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
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
    settings.put("partitioner.class", classOf[MaxwellJoinPartitioner])
    settings
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


  val keySer      = MaxwellKeySerializer()
  val keyDeser    = MaxwellKeyDeserializer()
  val valSer      = JsonSerializer[MaxwellValue]()
  val valDeser    = JsonDeserializer[MaxwellValue]()
  val idMapSer    = JsonSerializer[MapByID]()
  val idMapDeser  = JsonDeserializer[MapByID]()
  val mxDataSer   = JsonSerializer[MaxwellData]()
  val mxDataDeser = JsonDeserializer[MaxwellData]()


  builder.addSource("maxwell-root", keyDeser, valDeser, "maxwell")


  val liftDataPSupplier = new BasicProcessorSupplier[MaxwellKey, MaxwellValue](
    (context, k, v) => context.forward(k, v.data)
  )

  builder.addProcessor("maxwell-data", liftDataPSupplier, "maxwell-root")
  builder.addProcessor("maxwell-tickets", TableFilter("tickets"), "maxwell-data")
  builder.addProcessor("maxwell-users", TableFilter("users"), "maxwell-data")

  val dataStore = Stores.create("maxwell-data")
    .withKeys(keySer, keyDeser)
    .withValues(valSer, valDeser)
    .persistent()
    .build()

  val linkageStore = Stores.create("maxwell-links")
    .withKeys(keySer, keyDeser)
    .withValues(JsonSerializer[List[MaxwellKey]], JsonDeserializer[List[MaxwellKey]])
    .persistent()
    .build()

  /*
  builder.addStateStore(dataStore,    "maxwell-join-tickets", "maxwell-join-users")
  builder.addStateStore(linkageStore, "maxwell-join-tickets", "maxwell-join-users")
  */
  /*

  val maxwellInputTable: KTable[MaxwellKey, MaxwellData] = builder
    .table(keySer, valSer, keyDeser, valDeser, "maxwell")
    .mapValues(MapMaxwellValue())

  class TicketFieldAggregator extends Aggregator[MaxwellKey, MapByID, MapByID] {
    override def apply(aggKey: MaxwellKey, value: MapByID, aggregate: MapByID): MapByID = {
      aggregate ++ value
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

  val ticketsTable: KTable[MaxwellKey, MaxwellData] =
    maxwellInputTable.filter(TableFilter("tickets"))

  ticketsTable.leftJoin(tfeAggregateTable, new ValueJoiner[MaxwellData, MapByID, MaxwellData] {
    override def apply(value1: MaxwellData, value2: MapByID): MaxwellData = {
      if ( value2 != null )
        value1 + ("ticket_field_entries" -> value2.values.toList)
      else
        value1
    }
  }).to("maxwell-join-final-tickets", keySer, mxDataSer)

  builder.stream(keyDeser, mxDataDeser, "maxwell-join-final-tickets").map(
    new KeyValueMapper[MaxwellKey, MaxwellData, KeyValue[MaxwellKey, MaxwellData]]() {
      override def apply(key: MaxwellKey, value: MaxwellData) = {
        println(s"k: $key, v: $value")
        new KeyValue(key, value)
      }
    }
  )
*/
  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}

/**
  * Implicit conversions that provide us with some syntactic sugar when writing stream transformations.
  */
object KeyValueImplicits {
  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)
}
