package com.zendesk.maxwelljoin


import java.util.Properties
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams._
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}


case class MaxwellKey(val database: String, val table: String, val fields: MaxwellRef)
case class MaxwellValue(val rowType: String, val database: String, val table: String,
                        val ts: BigInt, val xid: BigInt,
                        val data: Map[String, Any])

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

case class JoinDef(val thisField: String, val thatTable: String,
                   val thatAlias: String, val thatField: String,
                   val pointsRight: Boolean) {}

class JoinProcessor(joinDefs: List[JoinDef]) extends AbstractProcessor[MaxwellKey, MaxwellValue] {
  var dataStore: KeyValueStore[MaxwellKey, MaxwellData] = null
  var linkStore: KeyValueStore[MaxwellKey, Set[MaxwellRef]] = null

  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    dataStore = context.getStateStore(MaxwellJoin.DataStoreName).asInstanceOf[KeyValueStore[MaxwellKey, MaxwellData]]
    linkStore = context.getStateStore(MaxwellJoin.LinkStoreName).asInstanceOf[KeyValueStore[MaxwellKey, Set[MaxwellRef]]]
  }

  // if join field is not the same as the primary key, store
  // join field -> primary key reference

  // now lookup any foreign links, and inject into self or inject self into them

  def createReferenceLink(key: MaxwellKey, field: String, value: Any): Unit = {
    // database, table,   field,        value -> Set(MaxwellKey)
    // ...     , tickets, requester_id, 5     -> [ticket id 100, tickets 101]
    val refKey = MaxwellKey(key.database, key.table, List((field -> value)))
    val maybeRefSet = Option(linkStore.get(refKey))

    if (maybeRefSet.isEmpty || !maybeRefSet.get.contains(key.fields)) {
      val newSet = maybeRefSet.getOrElse(Set()) + key.fields
      linkStore.put(refKey, newSet)
    }
  }

  def processJoin(key: MaxwellKey, data: MaxwellData, join: JoinDef) : Option[MaxwellData] = {
    data.get(join.thisField).flatMap { refValue =>
      createReferenceLink(key, join.thisField, refValue)

      val maybeRefSet = Option(linkStore.get(MaxwellKey(key.database, join.thatTable, List(join.thatField -> refValue))))
      maybeRefSet.flatMap { refSet =>
        val refData = refSet.toList.map { ref =>
          val refKey = MaxwellKey(key.database, join.thatTable, ref)
          (refKey, Option(dataStore.get(refKey)))
        }

        if ( join.pointsRight ) {
          Some(data + (join.thatAlias -> refData.map(_._2).flatten))
        } else {
          refData.foreach { case (k, maybeData) =>
            maybeData.map { v =>
              val mValue = MaxwellValue("update", key.database, join.thatTable, System.currentTimeMillis() / 1000, 123, v)
              context.forward(k, mValue)
            }
          }
          None
        }
      }
    }
  }

  override def process(key: MaxwellKey, value: MaxwellValue): Unit = {
    dataStore.put(key, value.data)

    var data = value.data
    var mainRowChanged = false

    joinDefs.foreach { j =>
      processJoin(key, data, j).map { d =>
        data = d
        mainRowChanged = true
      }
    }

    if ( mainRowChanged ) {
      context.forward(key, data)
    }
  }
}

case class JoinProcessorSupplier(val baseTable: String, var upstreams: List[String]) extends ProcessorSupplier[MaxwellKey, MaxwellValue] {
  var joinDefs = List[JoinDef]()
  override def get() = new JoinProcessor(joinDefs)

  def join(thisField: String, thatTable: String, thatAlias: String, thatField: String, pointsRight: Boolean) = {
    joinDefs = joinDefs :+ JoinDef(thisField, thatTable, thatAlias, thatField, pointsRight)
  }

  val processorName = s"maxwell-join-process-$baseTable"
}

class JoinBuilder(builder: KStreamBuilder, baseTable: String, sourceTopic: String, sinkTopic: String) {
  val keySer      = JsonSerializer[MaxwellKey]()
  val keyDeser    = JsonDeserializer[MaxwellKey]()
  val valSer      = JsonSerializer[MaxwellValue]()
  val valDeser    = JsonDeserializer[MaxwellValue]()
  val mxDataSer   = JsonSerializer[MaxwellData]()
  val mxDataDeser = JsonDeserializer[MaxwellData]()

  val dataStore = Stores.create(MaxwellJoin.DataStoreName)
    .withKeys(keySer, keyDeser)
    .withValues(mxDataSer, mxDataDeser)
    .persistent()
    .build()

  val linkageStore = Stores.create(MaxwellJoin.LinkStoreName)
    .withKeys(keySer, keyDeser)
    .withValues(JsonSerializer[Set[MaxwellRef]], JsonDeserializer[Set[MaxwellRef]])
    .persistent()
    .build()

  builder.addStateStore(dataStore)
  builder.addStateStore(linkageStore)

  val joinProcessorSuppliers = collection.mutable.Map[String, JoinProcessorSupplier]()
  var processSupplierChain = List[JoinProcessorSupplier]()

  builder.addSource("maxwell-join-root", keyDeser, valDeser, sourceTopic)

  private val basePS = getJoinProcessorSupplier(baseTable)

  private def getJoinProcessorSupplier(table: String) = {
    joinProcessorSuppliers.getOrElseUpdate(table, {
      builder.addProcessor(s"maxwell-join-filter-$table", TableFilter(table), "maxwell-join-root")
      val ps = JoinProcessorSupplier(table, List(s"maxwell-join-filter-$table"))
      processSupplierChain = ps :: processSupplierChain
      ps
    })
  }

  def join(thisField: String, thatTable: String, thatAlias: String, thatField: String) = {
    val thisPS = getJoinProcessorSupplier(baseTable)
    val thatPS = getJoinProcessorSupplier(thatTable)

    thisPS.join(thisField, thatTable, thatAlias, thatField, true)
    thatPS.join(thatField, baseTable, "", thisField, false)

    thisPS.upstreams = thatPS.processorName :: thisPS.upstreams
  }

  def build = {
    processSupplierChain.foreach { ps =>
      val table = ps.baseTable
      builder.addProcessor(s"maxwell-join-process-$table", ps, ps.upstreams: _*)

      builder.connectProcessorAndStateStores(
        ps.processorName,
        MaxwellJoin.LinkStoreName,
        MaxwellJoin.DataStoreName
      )
    }

    builder.addSink("maxwell-join-output", sinkTopic, keySer, mxDataSer, s"maxwell-join-process-$baseTable")

  }
}

object MaxwellJoin extends App {
  val DataStoreName = "maxwell-join-data-store"
  val LinkStoreName = "maxwell-join-link-store"

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
