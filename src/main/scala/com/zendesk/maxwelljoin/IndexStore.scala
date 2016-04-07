package com.zendesk.maxwelljoin

import org.apache.kafka.streams.state.KeyValueStore


// this class provides access to a pair of key value stores that
// ape mysql's clustered index: data is kept in the `dataStore`,
// which is keyed into by the table's primary key,
// and `indexStore` maintains backwards pointers to that data
class IndexStore(dataStore: KeyValueStore[MaxwellKey, MaxwellData],
                 indexStore: KeyValueStore[MaxwellKey, Set[MaxwellRef]]) {

  def getData(key: MaxwellKey) = Option(dataStore.get(key))
  def putData(key: MaxwellKey, value: MaxwellData) = dataStore.put(key, value)
  def delData(key: MaxwellKey): Unit = dataStore.delete(key)

  private def getIndexRef(key: MaxwellKey): Set[MaxwellRef] = {
    Option(indexStore.get(key)).getOrElse(Set())
  }

  def getIndex(key: MaxwellKey): Set[MaxwellKey] = {
    getIndexRef(key).map { ref => key.withFields(ref) }
  }

  def putIndex(key: MaxwellKey, pk: MaxwellRef): Unit = {
    val indexSet = getIndexRef(key)

    if (!indexSet.contains(pk)) {
      val newSet = indexSet + pk
      indexStore.put(key, newSet)
    }
  }


  def delIndex(key: MaxwellKey, pk: MaxwellRef): Unit = {
    val indexSet = getIndexRef(key) - pk
    if ( indexSet.isEmpty )
      indexStore.delete(key)
    else
      indexStore.put(key, indexSet)
  }

  def getJoinData(key: MaxwellKey, joinValue: Any, join: JoinDef, isPKLookup: Boolean): List[(MaxwellKey, MaxwellData)] = {
    val lookupKey = MaxwellKey(key.database, join.thatTable, List(join.thatField -> joinValue))

    if ( isPKLookup ) {
      getData(lookupKey).map { d => List(lookupKey -> d)}.getOrElse(List())
    } else {
      getIndex(key).toList.flatMap { primaryKey =>
        getData(primaryKey).map(primaryKey -> _)
      }
    }
  }
}

object IndexStore {
  def apply(dataStore: KeyValueStore[MaxwellKey, MaxwellData],
                   indexStore: KeyValueStore[MaxwellKey, Set[MaxwellRef]]) =
    new IndexStore(dataStore, indexStore)
}
