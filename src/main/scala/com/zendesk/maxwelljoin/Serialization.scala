package com.zendesk.maxwelljoin

import java.util
import com.zendesk.maxwelljoin.mawxwelljoin.MapByID
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization._
import org.json4s.{FieldSerializer, JObject}
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
    val jvalue = parseJson(new String(data))
    jvalue match {
      case m: JObject =>
        val tblDB = m.extract[MaxwellTableDatabase]
        val pkList = m.values.filterKeys(_.startsWith("pk.")).toList.sortBy(_._1).map(_._2)
        MaxwellKey(tblDB.table, tblDB.database, pkList)
      case _ => throw new SerializationException("expected j-object as key")
    }
  }
}

case class MaxwellValueDeserializer() extends BasicDeserializer[MaxwellValue] {
  override implicit val formats = org.json4s.DefaultFormats + renameMaxwellRowType
  override def deserialize(s: String, bytes: Array[Byte]): MaxwellValue = {
    val str = new String(bytes)
    parseJson(str).extract[MaxwellValue]
  }
}

case class RowKeySerializer() extends BasicSerializer[MaxwellKey] {
  override def serialize(s: String, t: MaxwellKey): Array[Byte] =  {
    val l = List(t.database, t.table) ++ t.pk
    l.mkString("...").getBytes
  }
}

case class RowKeyDeserializer() extends BasicDeserializer[MaxwellKey] {
  override def deserialize(topic: String, data: Array[Byte]): MaxwellKey = {
    val s = new String(data)
    val l : List[String] = s.split("\\.\\.\\.").toList
    MaxwellKey(l(0), l(1), l.slice(2, l.size))
  }
}

class MapByIDDeserializer extends BasicDeserializer[MapByID] {
  override def deserialize(topic: String, bytes: Array[Byte]): MapByID = {
    if ( bytes == null )
      return null

    parseJson(new String(bytes)).extract[MapByID]
  }
}

class MapByIDSerializer extends BasicSerializer[MapByID] {
  override def serialize(s: String, t: MapByID) =
    write(t).getBytes
}

case class JsonSerializer[T <: AnyRef](implicit val manifest: Manifest[T]) extends BasicSerializer[T] {
  override def serialize(topic: String, obj: T): Array[Byte] = {
    write[T](obj).getBytes
  }
}

case class JsonDeserializer[T >: Null](implicit val manifest: Manifest[T]) extends BasicDeserializer[T] {
  override def deserialize(s: String, bytes: Array[Byte]): T = {
    if ( bytes == null ) return null
    parseJson(new String(bytes)).extract[T]
  }
}





