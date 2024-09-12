/*
 * The MIT License (MIT)
 */

package com.acervera.osm4scala.spark

import com.google.protobuf.ByteString
import de.topobyte.osm4j.core.access.OsmOutputStream
import de.topobyte.osm4j.core.model.iface._
import de.topobyte.osm4j.core.model.impl.{Bounds, Metadata}
import de.topobyte.osm4j.pbf.Compression
import de.topobyte.osm4j.pbf.protobuf.Osmformat.Relation.MemberType
import de.topobyte.osm4j.pbf.protobuf.Osmformat._
import de.topobyte.osm4j.pbf.seq.BlockWriter
import de.topobyte.osm4j.pbf.util.{PbfUtil, StringTable}

import java.io.{IOException, OutputStream}
import java.util

class CustomOsmPbfWriter(val output: OutputStream, var writeMetadata: Boolean) extends BlockWriter(output) with OsmOutputStream {

  private val TEN_POW_SEVEN = Math.pow(10, 7)
  private val WORLD_BOUNDS = new Bounds(-180d, 180d, 90d, -90d)
  private val EMPTY_METADATA = new Metadata(-1, -1L, -1L, "", -1L)

  private var headerWritten = false
  private var compression = Compression.DEFLATE
  private var useDense = true
  private var granularity = 100
  private var granularityFactor = TEN_POW_SEVEN
  private var dateGranularity = 1000
  private var batchLimit = 8000
  private var counter = 0

  private val stringTable = new StringTable
  private val bufNodes = new util.ArrayList[OsmNode]
  private val bufWays = new util.ArrayList[OsmWay]
  private val bufRelations = new util.ArrayList[OsmRelation]

  def getCompression: Compression = this.compression

  def setCompression(compression: Compression): Unit = this.compression = compression

  def isUseDense: Boolean = this.useDense

  def setUseDense(useDense: Boolean): Unit = this.useDense = useDense

  def getGranularity: Int = this.granularity

  def setGranularity(granularity: Int): Unit = {
    this.granularity = granularity
    this.granularityFactor = granularity * Math.pow(10, 5)
  }

  def getDateGranularity: Int = this.dateGranularity

  def setDateGranularity(dateGranularity: Int): Unit = this.dateGranularity = dateGranularity

  def getBatchLimit: Int = this.batchLimit

  def setBatchLimit(batchLimit: Int): Unit = this.batchLimit = batchLimit

  @throws[IOException]
  override def write(bounds: OsmBounds): Unit = {
    if (this.headerWritten) return
    this.headerWritten = true

    val headerByteString = PbfUtil.createHeader("osm4j-pbf-1.2.0", true, if (bounds == null) WORLD_BOUNDS else bounds).toByteString
    this.write("OSMHeader", null.asInstanceOf[ByteString], this.compression, headerByteString)
  }

  @throws[IOException]
  override def write(node: OsmNode): Unit = {
    bufNodes.add(node)
    this.incrementCounter()
  }

  @throws[IOException]
  override def write(way: OsmWay): Unit = {
    this.bufWays.add(way)
    this.incrementCounter()
  }

  @throws[IOException]
  override def write(relation: OsmRelation): Unit = {
    this.bufRelations.add(relation)
    this.incrementCounter()
  }

  @throws[IOException]
  private def incrementCounter(): Unit = {
    this.counter += 1
    if (this.counter >= this.batchLimit) {
      this.writeBatch()
    }
  }

  @throws[IOException]
  override def complete(): Unit = {
    this.ensureHeader()
    if (this.counter > 0) this.writeBatch()
  }

  @throws[IOException]
  private def writeBatch(): Unit = {
    this.ensureHeader()
    val primitiveBlockBuilder = PrimitiveBlock.newBuilder
    this.addTagsToStringTable(this.bufNodes)
    this.addTagsToStringTable(this.bufWays)
    this.addTagsToStringTable(this.bufRelations)
    this.addMemberRolesToStringTable(this.bufRelations)
    if (this.writeMetadata) {
      this.addUsersToStringTable(this.bufNodes)
      this.addUsersToStringTable(this.bufWays)
      this.addUsersToStringTable(this.bufRelations)
    }
    this.stringTable.finish()
    if (!this.bufNodes.isEmpty) {
      primitiveBlockBuilder.addPrimitivegroup(if (this.useDense) this.serializeDense(this.bufNodes) else this.serializeNonDense(this.bufNodes))
      this.bufNodes.clear()
    }
    if (!this.bufWays.isEmpty) {
      primitiveBlockBuilder.addPrimitivegroup(this.serializeWays(this.bufWays))
      this.bufWays.clear()
    }
    if (!this.bufRelations.isEmpty) {
      primitiveBlockBuilder.addPrimitivegroup(this.serializeRelations(this.bufRelations))
      this.bufRelations.clear()
    }
    primitiveBlockBuilder.setDateGranularity(this.dateGranularity)
    primitiveBlockBuilder.setGranularity(this.granularity)
    primitiveBlockBuilder.setStringtable(this.stringTable.serialize)
    val primitiveBlockByteString = primitiveBlockBuilder.build.toByteString
    this.counter = 0
    this.stringTable.clear()
    this.write("OSMData", null.asInstanceOf[ByteString], this.compression, primitiveBlockByteString)
  }

  @throws[IOException]
  private def ensureHeader(): Unit = this.write(null.asInstanceOf[OsmBounds])

  private def addTagsToStringTable(entities: util.Collection[_ <: OsmEntity]): Unit = {
    entities.forEach(entity => {
      for (tagIdx <- 0 until entity.getNumberOfTags) {
        val tag = entity.getTag(tagIdx)
        this.stringTable.incr(tag.getKey)
        this.stringTable.incr(tag.getValue)
      }
    })
  }

  private def addUsersToStringTable(entities: util.Collection[_ <: OsmEntity]): Unit = {
    entities.forEach(entity => {
      val metadata = entity.getMetadata
      if (metadata != null) {
        val user = metadata.getUser
        if (user != null) this.stringTable.incr(user)
      }
    })
  }

  private def serializeDense(nodes: util.Collection[OsmNode]): PrimitiveGroup = {
    val primitiveGroupBuilder = PrimitiveGroup.newBuilder
    val denseNodeBuilder = DenseNodes.newBuilder

    var lastLat = 0.0
    var lastLon = 0.0
    var lastId = 0L

    var hasNodeMetadata: Boolean = false
    nodes.forEach(node => {
      hasNodeMetadata = hasNodeMetadata || node.getMetadata != null

      denseNodeBuilder.addId(node.getId - lastId)
      lastId = node.getId

      denseNodeBuilder.addLat(this.degrees(node.getLatitude, lastLat))
      lastLat = node.getLatitude

      denseNodeBuilder.addLon(this.degrees(node.getLongitude, lastLon))
      lastLon = node.getLongitude

      for (tagIdx <- 0 until node.getNumberOfTags) {
        val denseNodeTag = node.getTag(tagIdx)
        denseNodeBuilder.addKeysVals(this.stringTable.getIndex(denseNodeTag.getKey))
        denseNodeBuilder.addKeysVals(this.stringTable.getIndex(denseNodeTag.getValue))
      }
      denseNodeBuilder.addKeysVals(0)
    })

    if (this.writeMetadata && hasNodeMetadata) {
      val denseInfoBuilder = DenseInfo.newBuilder
      this.serializeMetadataDense(denseInfoBuilder, nodes)
      denseNodeBuilder.setDenseinfo(denseInfoBuilder)
    }

    primitiveGroupBuilder.setDense(denseNodeBuilder)
    primitiveGroupBuilder.build
  }

  private def serializeMetadataDense(denseInfoBuilder: DenseInfo.Builder, entities: util.Collection[_ <: OsmEntity]): Unit = {
    var lastTimestamp = 0L
    var lastChangeset = 0L
    var lastUserSid = 0
    var lastUid = 0
    var userSid = 0
    val entitiesIter = entities.iterator
    while ( {
      entitiesIter.hasNext
    }) {
      val metadata = Option(entitiesIter.next.getMetadata).getOrElse(EMPTY_METADATA)
      val uid = metadata.getUid.toInt
      userSid = this.stringTable.getIndex(metadata.getUser)
      val timestamp = (metadata.getTimestamp / this.dateGranularity.toLong).toInt
      val version = metadata.getVersion
      val changeset = metadata.getChangeset
      denseInfoBuilder.addVersion(version)

      denseInfoBuilder.addTimestamp(timestamp.toLong - lastTimestamp)
      lastTimestamp = timestamp.toLong

      denseInfoBuilder.addChangeset(changeset - lastChangeset)
      lastChangeset = changeset

      denseInfoBuilder.addUid(uid - lastUid)
      lastUid = uid

      denseInfoBuilder.addUserSid(userSid - lastUserSid)
      lastUserSid = userSid
    }
  }

  private def degrees(currentCoord: Double, lastCoord: Double): Long = Math.round(this.granularityFactor * (currentCoord - lastCoord))

  private def serializeNonDense(nodes: util.Collection[OsmNode]): PrimitiveGroup = {
    val primitiveGroupBuilder = PrimitiveGroup.newBuilder
    nodes.forEach(node => {
      val nodeBuilder = Node.newBuilder
      nodeBuilder.setId(node.getId)
      nodeBuilder.setLon(this.mapDegrees(node.getLongitude))
      nodeBuilder.setLat(this.mapDegrees(node.getLatitude))
      for (tagIdx <- 0 until node.getNumberOfTags) {
        val nodeTag = node.getTag(tagIdx)
        nodeBuilder.addKeys(this.stringTable.getIndex(nodeTag.getKey))
        nodeBuilder.addVals(this.stringTable.getIndex(nodeTag.getValue))
      }
      if (this.writeMetadata) {
        val nodeMetadata = node.getMetadata
        if (nodeMetadata != null) nodeBuilder.setInfo(this.serializeMetadata(nodeMetadata))
      }
      primitiveGroupBuilder.addNodes(nodeBuilder)
    })
    primitiveGroupBuilder.build
  }

  private def mapDegrees(degrees: Double): Long = (degrees * TEN_POW_SEVEN).toLong

  private def serializeWays(ways: util.Collection[OsmWay]): PrimitiveGroup = {
    val primitiveGroupBuilder = PrimitiveGroup.newBuilder
    ways.forEach(way => {
      val wayBuilder = Way.newBuilder()
      wayBuilder.setId(way.getId)
      var lastNodeId = 0L
      for (nodeIdx <- 0 until way.getNumberOfNodes) {
        val nodeId = way.getNodeId(nodeIdx)
        wayBuilder.addRefs(nodeId - lastNodeId)
        lastNodeId = nodeId
      }
      for (tagIdx <- 0 until way.getNumberOfTags) {
        val wayTag = way.getTag(tagIdx)
        wayBuilder.addKeys(this.stringTable.getIndex(wayTag.getKey))
        wayBuilder.addVals(this.stringTable.getIndex(wayTag.getValue))
      }
      if (this.writeMetadata) {
        val wayMetadata = way.getMetadata
        if (wayMetadata != null) wayBuilder.setInfo(this.serializeMetadata(wayMetadata))
      }
      primitiveGroupBuilder.addWays(wayBuilder)
    })

    primitiveGroupBuilder.build
  }

  private def addMemberRolesToStringTable(relations: util.Collection[OsmRelation]): Unit = {
    relations.forEach(relation => {
      for (memberIdx <- 0 until relation.getNumberOfMembers) {
        this.stringTable.incr(relation.getMember(memberIdx).getRole)
      }
    })
  }

  private def serializeRelations(relations: util.Collection[OsmRelation]): PrimitiveGroup = {
    val primitiveGroupBuilder = PrimitiveGroup.newBuilder

    relations.forEach(relation => {
      val relationBuilder = Relation.newBuilder
      relationBuilder.setId(relation.getId)

      var lastMemberId = 0L
      for (memberIdx <- 0 until relation.getNumberOfMembers) {
        val member = relation.getMember(memberIdx)

        val memberId = member.getId
        relationBuilder.addMemids(memberId - lastMemberId)
        lastMemberId = memberId

        relationBuilder.addTypes(this.getMemberType(member.getType))
        relationBuilder.addRolesSid(this.stringTable.getIndex(member.getRole))
      }
      for (tagIdx <- 0 until relation.getNumberOfTags) {
        val relationTag = relation.getTag(tagIdx)
        relationBuilder.addKeys(this.stringTable.getIndex(relationTag.getKey))
        relationBuilder.addVals(this.stringTable.getIndex(relationTag.getValue))
      }
      if (this.writeMetadata) {
        val relationMetadata = relation.getMetadata
        if (relationMetadata != null) relationBuilder.setInfo(this.serializeMetadata(relationMetadata))
      }
      primitiveGroupBuilder.addRelations(relationBuilder)
    })
    primitiveGroupBuilder.build
  }

  private def serializeMetadata(metadata: OsmMetadata): Info.Builder = {
    assert(this.writeMetadata)
    val infoBuilder = Info.newBuilder
    if (metadata.getUid >= 0L) {
      infoBuilder.setUid(metadata.getUid.toInt)
      infoBuilder.setUserSid(this.stringTable.getIndex(metadata.getUser))
    }
    infoBuilder.setTimestamp(metadata.getTimestamp / this.dateGranularity)
    infoBuilder.setVersion(metadata.getVersion)
    infoBuilder.setChangeset(metadata.getChangeset)
    infoBuilder
  }

  private def getMemberType(entityType: EntityType): MemberType = entityType match {
    case EntityType.Node => MemberType.NODE
    case EntityType.Way => MemberType.WAY
    case EntityType.Relation => MemberType.RELATION
  }
}
