/*
 * The MIT License (MIT)
 */

package com.acervera.osm4scala.spark.model

import com.acervera.osm4scala.spark.OsmSqlEntity.{ENTITY_TYPE_NODE, ENTITY_TYPE_RELATION, ENTITY_TYPE_WAY}

import java.time.Instant

case class OsmEntity(
                      id: Long,
                      `type`: Byte,
                      latitude: Option[Double],
                      longitude: Option[Double],
                      nodes: Seq[Long] = Seq.empty,
                      relations: Seq[RelationMember] = Seq.empty,
                      tags: Map[String, String],
                      info: Option[Info])

object OsmEntity {
  def node(id: Long, latitude: Double, longitude: Double, tags: Map[String, String], info: Info = Info()): OsmEntity = {
    OsmEntity(id = id, `type` = ENTITY_TYPE_NODE, latitude = Some(latitude), longitude = Some(longitude), tags = tags, nodes = Seq.empty, relations = Seq.empty, info = Some(info))
  }

  def way(id: Long, tags: Map[String, String], nodes: Seq[Long], info: Info = Info()): OsmEntity = {
    OsmEntity(id = id, `type` = ENTITY_TYPE_WAY, latitude = None, longitude = None, tags = tags, nodes = nodes, relations = Seq.empty, info = Some(info))
  }

  def relation(id: Long, tags: Map[String, String], members: Seq[RelationMember], info: Info = Info()): OsmEntity = {
    OsmEntity(id = id, `type` = ENTITY_TYPE_RELATION, latitude = None, longitude = None, tags = tags, nodes = Seq.empty, relations = members, info = Some(info))
  }
}

case class RelationMember(id: Long, relationType: Byte, role: String)

case class Info(
                 version: Option[Int] = None,
                 timestamp: Option[Instant] = None,
                 changeset: Option[Long] = None,
                 userId: Option[Int] = None,
                 userName: Option[String] = None,
                 visible: Option[Boolean] = None)
