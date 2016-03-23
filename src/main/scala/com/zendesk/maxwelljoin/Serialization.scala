package com.zendesk.maxwelljoin

import java.util
import org.apache.kafka.common.serialization._
import org.json4s.JsonAST.JString
import org.json4s.{JValue, FieldSerializer, JArray}
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
  def extract(obj: List[JValue]) = {
    val List(JString(db), JString(tbl), JArray(pkList)) = obj
    val mapList = pkList.map { m => m.extract[Map[String, Any]] }
    MaxwellKey(db, tbl, mapList)
  }

  override def deserialize(topic: String, data: Array[Byte]): MaxwellKey = {
    val JArray(list) = parseJson(new String(data))
    extract(list)
  }
}

case class MaxwellKeySerializer() extends BasicSerializer[MaxwellKey] {
  def toList(key: MaxwellKey) = {
    List(key.database, key.table, key.pk)
  }

  override def serialize(topic: String, key: MaxwellKey) = {
    write(toList(key)).getBytes
  }
}

case class MaxwellLinkKeySerializer() extends BasicSerializer[MaxwellLinkKey] {
  val mks = MaxwellKeySerializer()
  override def serialize(topic: String, linkKey: MaxwellLinkKey) = {
    write(List(mks.toList(linkKey.from), linkKey.toTable)).getBytes
  }
}

case class MaxwellLinkKeyDeserializer() extends BasicDeserializer[MaxwellLinkKey] {
  val mkd = MaxwellKeyDeserializer()

  override def deserialize(topic: String, bytes: Array[Byte]): MaxwellLinkKey = {
    val JArray(list) = parseJson(new String(bytes))

    val JArray(fromKey) = list(0)
    val JString(toTable) = list(1)
    val key = mkd.extract(fromKey)

    MaxwellLinkKey(key, toTable)
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





