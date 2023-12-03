package com.milvus.spark.connector

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

case class MilvusBatch(fileSchema: StructType) extends Batch{
  private lazy val inputPartitions: Seq[MilvusPartition] = {
    Seq(MilvusPartition(0, 0, 1))
  }
  override def planInputPartitions(): Array[InputPartition] = {
    inputPartitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    MilvusScanPartitionReaderFactory()
  }
}
