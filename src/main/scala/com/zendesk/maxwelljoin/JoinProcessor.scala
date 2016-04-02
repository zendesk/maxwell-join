package com.zendesk.maxwelljoin

import org.apache.kafka.streams.processor.{ProcessorContext, AbstractProcessor}
import org.apache.kafka.streams.state.KeyValueStore

class JoinProcessor(joinDefs: List[JoinDef]) extends AbstractProcessor[MaxwellKey, MaxwellValue] {
  var dataStore: KeyValueStore[MaxwellKey, MaxwellData] = null
  var indexStore: KeyValueStore[MaxwellKey, Set[MaxwellRef]] = null
  var mdStore:   KeyValueStore[DBAndTable, List[String]] = null

  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    dataStore = context.getStateStore(MaxwellJoin.DataStoreName).asInstanceOf[KeyValueStore[MaxwellKey, MaxwellData]]
    indexStore = context.getStateStore(MaxwellJoin.LinkStoreName).asInstanceOf[KeyValueStore[MaxwellKey, Set[MaxwellRef]]]
    mdStore   = context.getStateStore(MaxwellJoin.MetadataStoreName).asInstanceOf[KeyValueStore[DBAndTable, List[String]]]
  }

  lazy private val tableInfo = TableInformation(mdStore)

  // if join field is not the same as the primary key, store
  // join field -> primary key reference

  def createIndexEntry(key: MaxwellKey, field: String, value: Any): Unit = {
    // database, table,   field,        value -> Set(MaxwellKey)
    // ...     , tickets, requester_id->5     -> [ticket id 100, tickets 101]
    val refKey = MaxwellKey(key.database, key.table, List((field -> value)))
    val maybeRefSet = Option(indexStore.get(refKey))

    if (maybeRefSet.isEmpty || !maybeRefSet.get.contains(key.fields)) {
      val newSet = maybeRefSet.getOrElse(Set()) + key.fields
      indexStore.put(refKey, newSet)
    }
  }

  def isPK(database: String, table: String, field: String) = {
    tableInfo.getPKFields(database, table) == Some(List(field))
  }

  def lookupDataByPK(key: MaxwellKey) = {
    Option(dataStore.get(key))
  }

  def lookupDataByIndex(indexKey: MaxwellKey) = {
    /* indirect lookup: go through the indexStore to get a list of data,
     * eg lookup ticket_field_entries by ticket_id */
    val maybeIndexSet = Option(indexStore.get(indexKey))
    maybeIndexSet.map { indexes =>
      indexes.toList.flatMap { idx =>
        val dataKey = MaxwellKey(indexKey.database, indexKey.table, idx)
        lookupDataByPK(dataKey).map(dataKey -> _)
      }
    }.getOrElse(List())
  }

  def getJoinData(key: MaxwellKey, joinValue: Any, join: JoinDef, isPKLookup: Boolean): List[(MaxwellKey, MaxwellData)] = {
    val lookupKey = MaxwellKey(key.database, join.thatTable, List(join.thatField -> joinValue))

    if ( isPKLookup ) {
      lookupDataByPK(lookupKey).map { d => List(lookupKey -> d)}.getOrElse(List())
    } else {
      lookupDataByIndex(lookupKey)
    }
  }

  def processJoin(key: MaxwellKey, data: MaxwellData, join: JoinDef) : Option[MaxwellData] = {
    data.get(join.thisField).flatMap { refValue =>
      if ( !isPK(key.database, key.table, join.thisField))
        createIndexEntry(key, join.thisField, refValue)

      val isJoinToPK = isPK(key.database, join.thatTable, join.thatField)
      val joinData = getJoinData(key, refValue, join, isJoinToPK)

      if ( join.pointsRight ) {
        if ( isJoinToPK ) {
          // has-one
          Some(data + (join.thatAlias -> joinData.headOption.map(_._2)))
        } else {
          // has-many
          Some(data + (join.thatAlias -> joinData.map(_._2)))
        }
      } else {
        /*
           we've put ourselves into the data store, now re-output
           the left hand records by sending them through as a virtual update
         */
        joinData.foreach { case (k, d) =>
          val mValue = MaxwellValue("update", key.database, join.thatTable, System.currentTimeMillis() / 1000, 123, d)
          context.forward(k, mValue)
        }
        None
      }
    }
  }

  override def process(key: MaxwellKey, value: MaxwellValue): Unit = {
    dataStore.put(key, value.data)
    tableInfo.setPKFields(key.database, key.table, key.pkFields)

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

