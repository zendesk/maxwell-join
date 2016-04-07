package com.zendesk.maxwelljoin

import org.apache.kafka.streams.processor.{ProcessorContext, AbstractProcessor}
import org.apache.kafka.streams.state.KeyValueStore

class UpdateProcessor(joinDefs: List[JoinDef]) extends AbstractJoinProcessor  {
  private def indexKey(key: MaxwellKey, field: String, value: Any) =
    MaxwellKey(key.database, key.table, List((field -> value)))

  def removeIndexEntry(key: MaxwellKey, field: String, value: Any): Unit = {
    val idxKey = indexKey(key, field, value)

    val dataKeySet = Option(idxStore.get(idxKey)).getOrElse(Set()) - key.fields

    if ( dataKeySet.isEmpty )
      idxStore.delete(idxKey)
    else
      idxStore.put(idxKey, dataKeySet)
  }

  // store join field -> primary key reference, ie
  // tickets.requester_id: 5 -> ticket 1, ticket 2
  def createIndexEntry(key: MaxwellKey, field: String, value: Any): Unit = {
    val maybeDataKeySet = Option(idxStore.get(indexKey(key, field, value)))

    if (maybeDataKeySet.isEmpty || !maybeDataKeySet.get.contains(key.fields)) {
      val newSet = maybeDataKeySet.getOrElse(Set()) + key.fields
      idxStore.put(indexKey(key, field, value), newSet)
    }
  }

  def isPK(database: String, table: String, field: String) = {
    tableInfo.getPKFields(database, table) == Some(List(field))
  }


  def processRightPointingJoin(key: MaxwellKey, data: MaxwellData, join: JoinDef): MaxwellData = {
    data.get(join.thisField).map { refValue =>
      val isJoinToPK = isPK(key.database, join.thatTable, join.thatField)
      val joinData = indexStore.getJoinData(key, refValue, join, isJoinToPK)

      if (isJoinToPK) {
        // has-one
        data + (join.thatAlias -> joinData.headOption.map(_._2))
      } else {
        // has-many
        data + (join.thatAlias -> joinData.map(_._2))
      }
    }.getOrElse(data)
  }

  // returns a set of primary-key lookup keys that should be replayed
  def getReplays(key: MaxwellKey, data: MaxwellData): Set[MaxwellKey] = {
    leftHandJoins.foldLeft(Set[MaxwellKey]()) { (set, join) =>
      data.get(join.thisField).map { joinValue =>
        val lookupKey = MaxwellKey(key.database, join.thatTable, List(join.thatField -> joinValue))
        if ( isPK(key.database, join.thatTable, join.thatField) )
          set + lookupKey
        else
          set ++: indexStore.getPrimaryKeys(lookupKey)
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
      indexStore.getDataByPrimaryKey(replay) map { data =>
        val mValue = MaxwellValue("replay", replay.database, replay.table, System.currentTimeMillis() / 1000, 0, data, None)
        context.forward(replay, mValue)
      }
    }

    if ( rightHandJoins.size > 0 ) {
      context.forward(key, newData)
    }
  }
}

