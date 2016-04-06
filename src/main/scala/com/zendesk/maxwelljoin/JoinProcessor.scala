package com.zendesk.maxwelljoin

import org.apache.kafka.streams.processor.{ProcessorContext, AbstractProcessor}
import org.apache.kafka.streams.state.KeyValueStore

class JoinProcessor(joinDefs: List[JoinDef], val rootProcessor: Boolean) extends AbstractProcessor[MaxwellKey, MaxwellValue] {
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


  private def indexKey(key: MaxwellKey, field: String, value: Any) =
    MaxwellKey(key.database, key.table, List((field -> value)))

  def removeIndexEntry(key: MaxwellKey, field: String, value: Any): Unit = {
    val idxKey = indexKey(key, field, value)

    val dataKeySet = Option(indexStore.get(idxKey)).getOrElse(Set()) - key.fields

    if ( dataKeySet.isEmpty )
      indexStore.delete(idxKey)
    else
      indexStore.put(idxKey, dataKeySet)
  }

  // if join field is not the same as the primary key, store
  // join field -> primary key reference
  def createIndexEntry(key: MaxwellKey, field: String, value: Any): Unit = {
    // database, table,   field,        value -> Set(MaxwellKey)
    // ...     , tickets, requester_id->5     -> [ticket id 100, tickets 101]
    val maybeDataKeySet = Option(indexStore.get(indexKey(key, field, value)))

    if (maybeDataKeySet.isEmpty || !maybeDataKeySet.get.contains(key.fields)) {
      val newSet = maybeDataKeySet.getOrElse(Set()) + key.fields
      indexStore.put(indexKey(key, field, value), newSet)
    }
  }

  def isPK(database: String, table: String, field: String) = {
    tableInfo.getPKFields(database, table) == Some(List(field))
  }

  private def lookupDataByPK(key: MaxwellKey) = {
    Option(dataStore.get(key))
  }

  private def lookupDataKeysByIndex(indexKey: MaxwellKey) = {
    Option(indexStore.get(indexKey)).map { refSet =>
      refSet.map { ref =>
        MaxwellKey(indexKey.database, indexKey.table, ref)
      }
    }.getOrElse(Set())
  }

  /* indirect lookup: go through the indexStore to get a list of data,
   * eg lookup ticket_field_entries by ticket_id */
  private def lookupDataByIndex(indexKey: MaxwellKey) = {
    lookupDataKeysByIndex(indexKey).toList.flatMap { dataKey =>
      lookupDataByPK(dataKey).map(dataKey -> _)
    }
  }

  def getJoinData(key: MaxwellKey, joinValue: Any, join: JoinDef, isPKLookup: Boolean): List[(MaxwellKey, MaxwellData)] = {
    val lookupKey = MaxwellKey(key.database, join.thatTable, List(join.thatField -> joinValue))

    if ( isPKLookup ) {
      lookupDataByPK(lookupKey).map { d => List(lookupKey -> d)}.getOrElse(List())
    } else {
      lookupDataByIndex(lookupKey)
    }
  }

  def processRightPointingJoin(key: MaxwellKey, data: MaxwellData, join: JoinDef): MaxwellData = {
    data.get(join.thisField).map { refValue =>
      val isJoinToPK = isPK(key.database, join.thatTable, join.thatField)
      val joinData = getJoinData(key, refValue, join, isJoinToPK)

      if (isJoinToPK) {
        // has-one
        data + (join.thatAlias -> joinData.headOption.map(_._2))
      } else {
        // has-many
        data + (join.thatAlias -> joinData.map(_._2))
      }
    }.getOrElse(data)
  }

  def getReplays(key: MaxwellKey, data: MaxwellData): Set[MaxwellKey] = {
    leftHandJoins.foldLeft(Set[MaxwellKey]()) { (set, join) =>
      data.get(join.thisField).map { joinValue =>
        val lookupKey = MaxwellKey(key.database, join.thatTable, List(join.thatField -> joinValue))
        if ( isPK(key.database, join.thatTable, join.thatField) )
          set + lookupKey
        else
          set ++: lookupDataKeysByIndex(lookupKey)
      }.getOrElse(set)
    }
  }

  private def createIndexEntries(key: MaxwellKey, value: MaxwellValue): Unit = {
    joinDefs.foreach { join =>
      value.data.get(join.thisField).map { joinValue =>
        if (!isPK(key.database, key.table, join.thisField))
          createIndexEntry(key, join.thisField, joinValue)
      }
    }
  }

  private def removeOldIndexEntries(key: MaxwellKey, oldData: MaxwellData): Unit = {
    joinDefs.foreach { join =>
      oldData.get(join.thisField).map { oldJoinValue =>
        if (!isPK(key.database, key.table, join.thisField))
          removeIndexEntry(key, join.thisField, oldJoinValue)
      }
    }
  }

  private def processInsert(key: MaxwellKey, value: MaxwellValue) = {
    dataStore.put(key, value.data)
    createIndexEntries(key, value)
    getReplays(key, value.data)
  }

  private def processUpdate(key: MaxwellKey, value: MaxwellValue) = {
    dataStore.put(key, value.data)

    val oldReplays = getReplays(key, value.old.get)

    removeOldIndexEntries(key, value.old.get)

    createIndexEntries(key, value)

    oldReplays ++: getReplays(key, value.data)
  }

  private def processDelete(key: MaxwellKey, value: MaxwellValue) = {
    dataStore.delete(key)
    val oldReplays = getReplays(key, value.data)

    removeOldIndexEntries(key, value.data)
    oldReplays
  }

  private def processReplay(key: MaxwellKey, value: MaxwellValue) = {
    getReplays(key, value.data)
  }

  def leftHandJoins  = joinDefs.filterNot(_.pointsRight)
  def rightHandJoins = joinDefs.filter(_.pointsRight)

  override def process(key: MaxwellKey, value: MaxwellValue): Unit = {
    tableInfo.setPKFields(key.database, key.table, key.pkFields)

    // collect left-join data
    // do updates

    val replays = value.rowType match {
      case "insert" => processInsert(key, value)
      case "update" => processUpdate(key, value)
      case "delete" => processDelete(key, value)
      case "replay" => processReplay(key, value)
    }

    val newData = rightHandJoins.foldRight(value.data) { (join, data) =>
      processRightPointingJoin(key, data, join)

    }

    replays.foreach { replay =>
      lookupDataByPK(replay) map { data =>
        val mValue = MaxwellValue("replay", replay.database, replay.table, System.currentTimeMillis() / 1000, 0, data, None)
        context.forward(replay, mValue)
      }
    }

    if ( rightHandJoins.size > 0 ) {
      context.forward(key, newData)
    }
  }
}

