package com.zendesk.maxwelljoin

import org.apache.kafka.streams.state.KeyValueStore


// this class provides access to a pair of key value stores that
// ape mysql's clustered index: data is kept in the `dataStore`,
// which is keyed into by the table's primary key,
// and `indexStore` maintains backwards pointers to that data
class IndexStore(dataStore: KeyValueStore[MaxwellKey, MaxwellData],
                 indexStore: KeyValueStore[MaxwellKey, Set[MaxwellRef]]) {

  def getDataByPrimaryKey(key: MaxwellKey) = {
    Option(dataStore.get(key))
  }

  def getPrimaryKeys(indexKey: MaxwellKey) = {
    Option(indexStore.get(indexKey)).map { refSet =>
      refSet.map { ref =>
        MaxwellKey(indexKey.database, indexKey.table, ref)
      }
    }.getOrElse(Set())
  }

  def getJoinData(key: MaxwellKey, joinValue: Any, join: JoinDef, isPKLookup: Boolean): List[(MaxwellKey, MaxwellData)] = {
    val lookupKey = MaxwellKey(key.database, join.thatTable, List(join.thatField -> joinValue))

    if ( isPKLookup ) {
      getDataByPrimaryKey(lookupKey).map { d => List(lookupKey -> d)}.getOrElse(List())
    } else {
      getPrimaryKeys(key).toList.flatMap { primaryKey =>
        getDataByPrimaryKey(primaryKey).map(primaryKey -> _)
      }
    }
  }
}

object IndexStore {
  def apply(dataStore: KeyValueStore[MaxwellKey, MaxwellData],
                   indexStore: KeyValueStore[MaxwellKey, Set[MaxwellRef]]) =
    new IndexStore(dataStore, indexStore)
}
