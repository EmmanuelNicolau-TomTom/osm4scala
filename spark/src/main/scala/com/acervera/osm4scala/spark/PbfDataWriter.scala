/*
 * The MIT License (MIT)
 */

package com.acervera.osm4scala.spark

import com.acervera.osm4scala.spark.model.{Info, OsmElement, OsmElementTypes, RelationMemberType}
import com.slimjars.dist.gnu.trove.list.array.TLongArrayList
import de.topobyte.osm4j.core.model.iface.{EntityType, OsmTag}
import de.topobyte.osm4j.core.model.impl
import de.topobyte.osm4j.core.model.impl._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Deserializer
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.StructType

import java.io.OutputStream
import java.util
import scala.collection.JavaConverters._

case class PbfDataWriter(deserializer: Deserializer[OsmElement], outputStream: OutputStream, pbfWriter: CustomOsmPbfWriter) {

  def write(row: InternalRow): Unit = {
    val element = deserializer.apply(row)
    OsmElementTypes.apply(element.`type`) match {
      case OsmElementTypes.Node => writeNode(element)
      case OsmElementTypes.Way => writeWay(element)
      case OsmElementTypes.Relation => writeRelation(element)
    }
  }

  private def writeNode(element: OsmElement): Unit = {
    val longitude = element.longitude.getOrElse(throw new IllegalArgumentException(s"Node is missing longitude: $element"))
    val latitude = element.latitude.getOrElse(throw new IllegalArgumentException(s"Node is missing latitude: $element"))
    pbfWriter.write(new impl.Node(element.id, longitude, latitude, createTags(element), createMetadata(element)))
  }

  private def writeWay(element: OsmElement): Unit = {
    pbfWriter.write(new impl.Way(element.id, new TLongArrayList(element.nodes.toArray), createTags(element), createMetadata(element)))
  }

  private def createTags(element: OsmElement): util.ArrayList[OsmTag] = {
    val tags = new util.ArrayList[OsmTag]()
    if (element.tags != null && element.tags.nonEmpty) {
      element.tags.toSeq
        .sortBy(_._1)
        .foreach { case (key, value) => tags.add(new Tag(key, value)) }
    }
    tags
  }

  private def createMetadata(element: OsmElement): Metadata = {
    val info = element.info.getOrElse(Info())
    val timeInMilliSeconds = info.timestamp.map(ts => ts.toEpochMilli).getOrElse(0L)
    new Metadata(info.version.getOrElse(0), timeInMilliSeconds, info.userId.getOrElse(0).toLong, info.userName.orNull, info.changeset.getOrElse(0L))
  }

  private def writeRelation(element: OsmElement): Unit = {
    val members = element.relations
      .getOrElse(Seq.empty)
      .map(member => new RelationMember(member.id, toRelationType(member.relationType), member.role))
      .toList
      .asJava
    pbfWriter.write(new Relation(element.id, members, createTags(element), createMetadata(element)))
  }

  private def toRelationType(elementType: Int): EntityType = {
    RelationMemberType.apply(elementType) match {
      case RelationMemberType.Node => EntityType.Node
      case RelationMemberType.Way => EntityType.Way
      case RelationMemberType.Relation => EntityType.Relation
      case default => throw new IllegalArgumentException(s"No support for writing relation member value: $default")
    }
  }

  def close(): Unit = {
    pbfWriter.complete()
    outputStream.close()
  }
}

object PbfDataWriter {
  def apply(schema: StructType, os: OutputStream): PbfDataWriter = {
    val osmElementDeserializer = Encoders
      .product[OsmElement]
      .asInstanceOf[ExpressionEncoder[OsmElement]]
      .resolveAndBind(schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
      .createDeserializer()
    PbfDataWriter(osmElementDeserializer, os, new CustomOsmPbfWriter(os, true))
  }

}
