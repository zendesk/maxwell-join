package com.zendesk.maxwelljoin


import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams._
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}


case class MaxwellKey(val database: String, val table: String, val fields: MaxwellRef) {
  def pkFields = fields.map(_._1)
  def pkValues = fields.map(_._2)
}

case class MaxwellValue(val rowType: String, val database: String, val table: String,
                        val ts: BigInt, val xid: BigInt,
                        val data: Map[String, Any])
case class DBAndTable(val db: String, val table: String)

class BasicProcessorSupplier[K, V]( f: (ProcessorContext, K, V) => Unit ) extends ProcessorSupplier[K, V] {
  override def get(): Processor[K, V] = new BasicProcessor(f)
  class BasicProcessor[K, V](val f: (ProcessorContext, K, V) => Unit) extends AbstractProcessor[K, V] {
    override def process(key: K, value: V) = f(context, key, value)
  }
}

case class TableFilter(table: String) extends BasicProcessorSupplier[MaxwellKey, MaxwellValue](
  (context, key, value) =>
    if ( key.table == table ) context.forward(key, value)
)

object MaxwellJoin extends App {
  val DataStoreName     = "maxwell-join-data-store"
  val LinkStoreName     = "maxwell-join-link-store"
  val MetadataStoreName = "maxwell-join-metadata-store"

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
    settings
  }

  val joinBuilder = new JoinBuilder(builder, "tickets", "maxwell", "maxwell-tickets")

  joinBuilder.join("requester_id", "users", "requester", "id")
  joinBuilder.join("id", "ticket_field_entries", "ticket_field_entries", "ticket_id")
  joinBuilder.build

  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}
