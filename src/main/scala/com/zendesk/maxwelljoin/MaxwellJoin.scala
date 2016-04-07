package com.zendesk.maxwelljoin

import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams._
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import scala.language.implicitConversions

case class MaxwellKey(val database: String, val table: String, val fields: MaxwellRef) {
  def pkFields = fields.map(_._1)
  def pkValues = fields.map(_._2)

  def withFields(newFields: MaxwellRef) = MaxwellKey(database, table, newFields)
  def withFields(newField: (String, Any)) = MaxwellKey(database, table, List(newField))
}

case class MaxwellValue(val rowType: String, val database: String, val table: String,
  val ts: BigInt, val xid: Option[BigInt],
  val data: Map[String, Any], val old: Option[Map[String, Any]])
case class DBAndTable(val db: String, val table: String)

class BasicProcessorSupplier[K, V](f: (ProcessorContext, K, V) => Unit) extends ProcessorSupplier[K, V] {
  override def get(): Processor[K, V] = new BasicProcessor(f)
  class BasicProcessor[K, V](val f: (ProcessorContext, K, V) => Unit) extends AbstractProcessor[K, V] {
    override def process(key: K, value: V) = f(context, key, value)
  }
}

case class TableFilter(table: String) extends BasicProcessorSupplier[MaxwellKey, MaxwellValue](
  (context, key, value) =>
    if (key.table == table) context.forward(key, value)
)

object MaxwellJoin extends App {
  val DataStoreName = "maxwell-join-data-store"
  val LinkStoreName = "maxwell-join-link-store"
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
  joinBuilder.join(from = "tickets.requester_id", to = "users.id", as = "requester")
  joinBuilder.join(from = "tickets.id", to = "ticket_field_entries.ticket_id")
  joinBuilder.join(from = "requester.id", to = "cf_values.owner_id")
  joinBuilder.join(from = "requester.organization_id", to = "organizations.id")
  joinBuilder.build

  val stream: KafkaStreams = new KafkaStreams(builder, streamingConfig)
  stream.start()
}
