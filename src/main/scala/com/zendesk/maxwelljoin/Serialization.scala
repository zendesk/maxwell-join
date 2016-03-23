package com.zendesk.maxwelljoin

import java.util
import org.apache.kafka.common.serialization._
import org.json4s.JsonAST.JString
import org.json4s.{FieldSerializer, JArray}
import org.json4s.FieldSerializer._
import org.json4s.native.parseJson
import org.json4s.native.Serialization.write
import scala.language.implicitConversions

trait SerializationFormats {
  val renameMaxwellRowType = FieldSerializer[MaxwellValue](
    renameTo("rowType", "type"),
    renameFrom("type", "rowType")
  )
  implicit val formats = org.json4s.DefaultFormats + renameMaxwellRowType
}
abstract trait BasicDeserializer[T] extends Deserializer[T] with SerializationFormats {
  def close(): Unit = {}
  def configure(map: util.Map[String, _], b: Boolean): Unit = {}
}

abstract trait BasicSerializer[T] extends Serializer[T] with SerializationFormats {
  def close(): Unit = {}
  def configure(map: util.Map[String, _], b: Boolean): Unit = {}
}

case class MaxwellKeyDeserializer() extends BasicDeserializer[MaxwellKey] {
  override def deserialize(topic: String, data: Array[Byte]): MaxwellKey = {
    val JArray(List(JString(db), JString(tbl), JArray(pkList))) = parseJson(new String(data))
    val mapList = pkList.map { m => m.extract[Map[String, Any]] }
    MaxwellKey(db, tbl, mapList)
  }
}

case class MaxwellKeySerializer() extends BasicSerializer[MaxwellKey] {
  override def serialize(topic: String, key: MaxwellKey) = {
    write(List(key.database, key.table, key.pk)).getBytes
  }
}
case class JsonSerializer[T <: AnyRef](implicit val manifest: Manifest[T]) extends BasicSerializer[T] {
  override def serialize(topic: String, obj: T): Array[Byte] = {
    if ( obj == null )
      return null
    else
      write[T](obj).getBytes
  }
}

case class JsonDeserializer[T >: Null](implicit val manifest: Manifest[T]) extends BasicDeserializer[T] {
  override def deserialize(s: String, bytes: Array[Byte]): T = {
    if ( bytes == null ) return null
    parseJson(new String(bytes)).extract[T]
  }
}





