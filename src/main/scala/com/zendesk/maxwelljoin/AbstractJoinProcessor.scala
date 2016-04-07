package com.zendesk.maxwelljoin

import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore

abstract class AbstractJoinProcessor extends AbstractProcessor[MaxwellKey, MaxwellValue] {
  var dataStore: KeyValueStore[MaxwellKey, MaxwellData] = null
  var idxStore: KeyValueStore[MaxwellKey, Set[MaxwellRef]] = null
  var mdStore:   KeyValueStore[DBAndTable, List[String]] = null

  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    dataStore = context.getStateStore(MaxwellJoin.DataStoreName).asInstanceOf[KeyValueStore[MaxwellKey, MaxwellData]]
    idxStore = context.getStateStore(MaxwellJoin.LinkStoreName).asInstanceOf[KeyValueStore[MaxwellKey, Set[MaxwellRef]]]
    mdStore   = context.getStateStore(MaxwellJoin.MetadataStoreName).asInstanceOf[KeyValueStore[DBAndTable, List[String]]]
  }

  lazy protected val tableInfo = TableInformation(mdStore)
  lazy protected val indexStore = IndexStore(dataStore, idxStore)
}
