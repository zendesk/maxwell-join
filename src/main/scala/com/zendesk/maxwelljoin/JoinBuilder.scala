package com.zendesk.maxwelljoin

import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.state.Stores
import scala.language.implicitConversions

case class JoinDef(val thisTable: String, val thisAlias: String, val thisField: String,
  val thatTable: String, val thatAlias: String, val thatField: String,
  val pointsRight: Boolean) {}

class JoinBuilder(builder: KStreamBuilder, baseTable: String, sourceTopic: String, sinkTopic: String) {
  val keySer = JsonSerializer[MaxwellKey]()
  val keyDeser = JsonDeserializer[MaxwellKey]()
  val valSer = JsonSerializer[MaxwellValue]()
  val valDeser = JsonDeserializer[MaxwellValue]()
  val mxDataSer = JsonSerializer[MaxwellData]()
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

  val joinProcessorSuppliers = collection.mutable.Map[String, UpdateProcessorSupplier]()
  var processSupplierChain = List[UpdateProcessorSupplier]()

  builder.addSource("maxwell-join-root", keyDeser, valDeser, sourceTopic)

  private val basePS = getUpdateProcessorSupplier(baseTable, forwardSelf = true)
  private val joinerPS = new JoinProcessorSupplier()

  private def getUpdateProcessorSupplier(table: String, forwardSelf: Boolean) = {
    joinProcessorSuppliers.getOrElseUpdate(table, {
      builder.addProcessor(s"maxwell-join-filter-$table", TableFilter(table), "maxwell-join-root")
      val ps = UpdateProcessorSupplier(table, List(s"maxwell-join-filter-$table"), forwardSelf)
      processSupplierChain = ps :: processSupplierChain
      ps
    })
  }

  def join(thisTable: String, thisField: String, thisAlias: String, thatTable: String, thatField: String, thatAlias: String): Unit = {
    val thisPS = getUpdateProcessorSupplier(thisTable, forwardSelf = false)
    val thatPS = getUpdateProcessorSupplier(thatTable, forwardSelf = false)

    thisPS.join(thisField, thatTable, thatAlias, thatField, true)
    thatPS.join(thatField, thisTable, "", thisField, false)

    joinerPS.join(thisTable, thisAlias, thisField, thatTable, thatAlias, thatField)

    thisPS.upstreams = thatPS.processorName :: thisPS.upstreams
  }

  private val aliasMap = collection.mutable.Map[String, String]()

  def join(from: String, to: String, as: String = null): Unit = {
    val List(rawFromTable, fromField) = from.split("\\.").toList
    val List(toTable, toField) = to.split("\\.").toList
    val toAlias = Option(as).getOrElse(toTable)

    Option(as).map { a => aliasMap.put(a, toTable) }

    val (fromTable, fromAlias) = aliasMap.get(rawFromTable) match {
      case Some(tbl) => (tbl, rawFromTable)
      case None => (rawFromTable, rawFromTable)
    }

    join(fromTable, fromField, fromAlias, toTable, toField, toAlias)
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

    builder.addProcessor(s"maxwell-join-join-$baseTable", joinerPS, s"maxwell-join-process-$baseTable")
    builder.addSink("maxwell-join-output", sinkTopic, keySer, mxDataSer, s"maxwell-join-join-$baseTable")
  }
}

case class UpdateProcessorSupplier(
  val baseTable: String, var upstreams: List[String], val forwardSelf: Boolean
) extends ProcessorSupplier[MaxwellKey, MaxwellValue] {
  var joinDefs = List[JoinDef]()
  override def get() = new UpdateProcessor(joinDefs, forwardSelf)

  def join(thisField: String, thatTable: String, thatAlias: String, thatField: String, pointsRight: Boolean) = {
    joinDefs = joinDefs :+ JoinDef(baseTable, baseTable, thisField, thatTable, thatAlias, thatField, pointsRight)
  }

  val processorName = s"maxwell-join-process-$baseTable"
}

case class JoinProcessorSupplier() extends ProcessorSupplier[MaxwellKey, MaxwellValue] {
  var joinDefs = List[JoinDef]()
  override def get() = new JoinProcessor(joinDefs)

  def join(fromTable: String, fromAlias: String, fromField: String, toTable: String, toAlias: String, toField: String) = {
    joinDefs = joinDefs :+ JoinDef(fromTable, fromAlias, fromField, toTable, toAlias, toField, pointsRight = true)
  }
}
