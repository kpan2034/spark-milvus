package com.milvus.spark.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

case class MilvusScanPartitionReaderFactory() extends PartitionReaderFactory{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    MilvusScanPartitionReader(partition)
  }
}
