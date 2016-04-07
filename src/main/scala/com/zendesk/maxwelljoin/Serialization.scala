package com.zendesk.maxwelljoin

import java.util
import org.apache.kafka.common.serialization._
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.{CustomSerializer, JValue, FieldSerializer, JArray}
import org.json4s.FieldSerializer._
import org.json4s.native.parseJson
import org.json4s.native.Serialization.write
import org.json4s.Extraction.decompose
import scala.language.implicitConversions

class MaxwellKeySerializer extends CustomSerializer[MaxwellKey](format => (
  {
    case JArray(List(JString(db), JString(tbl), JArray(rawList))) =>
      implicit val formats = org.json4s.DefaultFormats
      val mapList = rawList.map { m => m.extract[Map[String, Any]] }
      val pkList = mapList.map { m => (m.keys.toList(0), m.values.toList(0)) }
      MaxwellKey(db, tbl, pkList)
  },
  {
    case k: MaxwellKey =>
      implicit val formats = org.json4s.DefaultFormats
      decompose(List(k.database, k.table, k.fields.map { pk => Map(pk._1 -> pk._2) }))
  }
))

trait SerializationFormats {
  val renameMaxwellRowType = FieldSerializer[MaxwellValue](
    renameTo("rowType", "type"),
    renameFrom("type", "rowType")
  )

  implicit val formats = org.json4s.DefaultFormats + renameMaxwellRowType + new MaxwellKeySerializer()
}

case class JsonSerializer[T <: AnyRef](implicit val manifest: Manifest[T])
  extends Serializer[T] with SerializationFormats {
  def close(): Unit = {}
  def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(topic: String, obj: T): Array[Byte] = {
    if (obj == null)
      return null
    else
      write[T](obj).getBytes
  }
}

case class JsonDeserializer[T >: Null](implicit val manifest: Manifest[T])
  extends Deserializer[T] with SerializationFormats {
  def close(): Unit = {}
  def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def deserialize(s: String, bytes: Array[Byte]): T = {
    if (bytes == null) return null
    parseJson(new String(bytes)).extract[T]
  }
}

