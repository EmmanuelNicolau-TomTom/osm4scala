/*
 * The MIT License (MIT)
 */

package com.acervera.osm4scala.spark.fc

object RelationMemberType extends Enumeration {

  type RelationMemberType = Value

  val Node: Value = Value(0, "node")
  val Way: Value = Value(1, "way")
  val Relation: Value = Value(2, "relation")

}
