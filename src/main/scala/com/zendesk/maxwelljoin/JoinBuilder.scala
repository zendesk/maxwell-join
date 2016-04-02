package com.zendesk.maxwelljoin

import org.apache.kafka.common.serialization.{ StringSerializer, StringDeserializer }
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.state.Stores

case class JoinDef(val thisField: String, val thatTable: String,
                   val thatAlias: String, val thatField: String,
                   val pointsRight: Boolean) {}

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

  val metadataStore = Stores.create(MaxwellJoin.MetadataStoreName)
    .withKeys(JsonSerializer[DBAndTable], JsonDeserializer[DBAndTable])
    .withValues(JsonSerializer[List[String]], JsonDeserializer[List[String]])
    .persistent()
    .build()

  builder.addStateStore(dataStore)
  builder.addStateStore(linkageStore)
  builder.addStateStore(metadataStore)

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
        MaxwellJoin.DataStoreName,
        MaxwellJoin.MetadataStoreName
      )
    }

    builder.addSink("maxwell-join-output", sinkTopic, keySer, mxDataSer, s"maxwell-join-process-$baseTable")

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


