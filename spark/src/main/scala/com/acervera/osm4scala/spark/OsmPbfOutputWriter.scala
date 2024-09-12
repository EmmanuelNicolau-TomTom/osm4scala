/*
 * The MIT License (MIT)
 */

package com.acervera.osm4scala.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

import java.io.OutputStream

case class OsmPbfOutputWriter(os: OutputStream, schema: StructType, writePath: String) extends OutputWriter {

  private val pbfDataWriter = PbfDataWriter(schema, os)

  override def write(row: InternalRow): Unit = pbfDataWriter.write(row)

  override def close(): Unit = pbfDataWriter.close()

  override def path(): String = writePath
}
