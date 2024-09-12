/*
 * The MIT License (MIT)
 */

package com.acervera.osm4scala.spark.model

case class OsmElement(
                       id: Long,
                       `type`: Byte,
                       latitude: Option[Double],
                       longitude: Option[Double],
                       nodes: Seq[Long] = Seq.empty,
                       relations: Option[Seq[RelationMember]] = Option(Seq.empty),
                       tags: Map[String, String],
                       info: Option[Info] = Some(Info()))

case class RelationMember(id: Long, relationType: Byte, role: String)

import java.time.Instant

case class Info(
                 version: Option[Int] = None,
                 timestamp: Option[Instant] = None,
                 changeset: Option[Long] = None,
                 userId: Option[Int] = None,
                 userName: Option[String] = None,
                 visible: Option[Boolean] = None)
