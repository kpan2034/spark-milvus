package com.milvus.spark.connector

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.Seq

case class MilvusBatch(conf: MilvusConnectorConf, collectionSchema: StructType) extends Batch{
  private lazy val inputPartitions: Seq[MilvusPartition] = {
    getInputPartitionInformation()
  }
  override def planInputPartitions(): Array[InputPartition] = {
    inputPartitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    MilvusScanPartitionReaderFactory(conf, collectionSchema)
  }

  private lazy val client = {
    MilvusClient(conf.host, conf.port)
  }

  private def getInputPartitionInformation(): Seq[MilvusPartition] = {
    val numPartitions: Int = conf.numPartitions

    val primaryKeys: Array[Long] = client.queryCollection(
      collectionName = conf.collectionName,
      fieldsToReturn = List("id"),
      query=conf.predicateFilter
    ).map(_.get("id").asInstanceOf[Long]).toArray
    val sortedPrimaryKeys = primaryKeys.sorted
    val numRecordsPerPartition = sortedPrimaryKeys.length/numPartitions

    var partitions: mutable.Seq[MilvusPartition] = mutable.Seq()

    for(i <- 0 until numPartitions-1) {
      val startKey = sortedPrimaryKeys.apply(i*numRecordsPerPartition)
      val endKey = sortedPrimaryKeys.apply((i+1)*numRecordsPerPartition - 1)

      partitions = partitions :+ MilvusPartition(i, startKey, endKey, collectionSchema)
    }

    partitions = partitions :+ MilvusPartition(
      numPartitions-1,
      sortedPrimaryKeys.apply((numPartitions-1)*numRecordsPerPartition),
      sortedPrimaryKeys.apply(sortedPrimaryKeys.length-1),
      collectionSchema
    )

    partitions
  }
}
