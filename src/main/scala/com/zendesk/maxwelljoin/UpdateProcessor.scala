package com.zendesk.maxwelljoin

import org.apache.kafka.streams.processor.{ProcessorContext, AbstractProcessor}
import org.apache.kafka.streams.state.KeyValueStore

class UpdateProcessor(joinDefs: List[JoinDef], forwardSelf: Boolean) extends AbstractJoinProcessor(joinDefs) {
  private def indexKey(key: MaxwellKey, field: String, value: Any) =
    MaxwellKey(key.database, key.table, List((field -> value)))

  def removeIndexEntry(key: MaxwellKey, field: String, value: Any): Unit = {
  }

  // returns a set of primary-key lookup keys that should be replayed
  def getReplays(key: MaxwellKey, data: MaxwellData): Set[MaxwellKey] = {
    leftHandJoins.foldLeft(Set[MaxwellKey]()) { (set, join) =>
      data.get(join.thisField).map { joinValue =>
        val lookupKey = MaxwellKey(key.database, join.thatTable, List(join.thatField -> joinValue))
        if (tableInfo.isKeyPrimary(key.database, join.thatTable, join.thatField))
          set + lookupKey
        else
          set ++: indexStore.getIndex(lookupKey)
      }.getOrElse(set)
    }
  }

  private def createIndexEntries(key: MaxwellKey, value: MaxwellValue): Unit = {
    joinDefs.foreach { join =>
      if (!tableInfo.isKeyPrimary(key.database, key.table, join.thisField)) {
        value.data.get(join.thisField).map { joinValue =>
          indexStore.putIndex(key.withFields(join.thisField -> joinValue), key.fields)
        }
      }
    }
  }

  private def removeOldIndexEntries(key: MaxwellKey, oldData: MaxwellData): Unit = {
    joinDefs.foreach { join =>
      if (!tableInfo.isKeyPrimary(key.database, key.table, join.thisField)) {
        oldData.get(join.thisField).map { oldJoinValue =>
          indexStore.delIndex(key.withFields(join.thisField -> oldJoinValue), key.fields)
          removeIndexEntry(key, join.thisField, oldJoinValue)
        }
      }
    }
  }

  private def processInsert(key: MaxwellKey, value: MaxwellValue) = {
    indexStore.putData(key, value.data)
    createIndexEntries(key, value)
    getReplays(key, value.data)
  }

  private def processUpdate(key: MaxwellKey, value: MaxwellValue) = {
    indexStore.putData(key, value.data)

    val oldReplays = getReplays(key, value.old.get)

    removeOldIndexEntries(key, value.old.get)

    createIndexEntries(key, value)

    oldReplays ++: getReplays(key, value.data)
  }

  private def processDelete(key: MaxwellKey, value: MaxwellValue) = {
    indexStore.delData(key)
    val oldReplays = getReplays(key, value.data)

    removeOldIndexEntries(key, value.data)
    oldReplays
  }

  private def processReplay(key: MaxwellKey, value: MaxwellValue) = {
    getReplays(key, value.data)
  }

  def leftHandJoins = joinDefs.filterNot(_.pointsRight)
  def rightHandJoins = joinDefs.filter(_.pointsRight)

  override def process(key: MaxwellKey, value: MaxwellValue): Unit = {
    tableInfo.setPKFields(key.database, key.table, key.pkFields)

    // collect left-join data
    // do updates

    val replays = value.rowType match {
      case "insert" | "bootstrap-insert" => processInsert(key, value)
      case "update" => processUpdate(key, value)
      case "delete" => processDelete(key, value)
      case "replay" => processReplay(key, value)

      case _ => Set()
    }

    replays.foreach { replay =>
      indexStore.getData(replay) map { data =>
        val mValue = MaxwellValue("replay", replay.database, replay.table, System.currentTimeMillis() / 1000, None, data, None)
        context.forward(replay, mValue)
      }
    }

    if ( forwardSelf ) {
      context.forward(key, value)
    }
  }
}

