package com.zendesk.maxwelljoin

import org.apache.kafka.streams.state.KeyValueStore

case class TableInformation(mdStore: KeyValueStore[DBAndTable, List[String]]) {
  val pkFields = collection.mutable.Map[DBAndTable, List[String]]()

  def isKeyPrimary(database: String, table: String, field: String) = {
    getPKFields(database, table) == Some(List(field))
  }

  def getPKFields(db: String, table: String): Option[List[String]] = {
    val key = DBAndTable(db, table)
    val value = pkFields.get(key)

    if (value.isDefined) {
      return value
    }

    Option(mdStore.get(key)).map { v =>
      pkFields.put(key, v)
      v
    }
  }

  def setPKFields(db: String, table: String, fields: List[String]): Unit = {
    val key = DBAndTable(db, table)
    val value = pkFields.get(key)
    if (value.isDefined && value.get == fields)
      return

    mdStore.put(key, fields)
    pkFields.put(key, fields)
  }
}
