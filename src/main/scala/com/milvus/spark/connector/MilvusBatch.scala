package com.milvus.spark.connector

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

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

  private def getInputPartitionInformation(): Seq[MilvusPartition] = {
    // TODO: figure out partition planning for the query

    // 1. Get number of rows in the collection (client.getCollectionRowCount)
    // and then then split partition accordingly (e.g. 1000 rows per partition)
    // in this case, MilvusPartition requires start and partitionLength
    // OR figure out how primary key value is distributed
    // in this case MilvusPartition will require Array[PrimaryKeyValues]

    // 2. Split it into a number of partitions
    // side TODO: how do you know how *many* partitions to split them into -- is it user specified? or is there a default?

    // 3. Build and return sequence (or array) of MilvusPartition objects
    Seq(MilvusPartition(0, 0, 1, collectionSchema))
  }
}
