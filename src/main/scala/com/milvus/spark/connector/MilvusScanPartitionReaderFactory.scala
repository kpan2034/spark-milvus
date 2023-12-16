package com.milvus.spark.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

case class MilvusScanPartitionReaderFactory(conf: MilvusConnectorConf, collectionSchema: StructType) extends PartitionReaderFactory{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    MilvusScanPartitionReader(conf, partition)
  }
}
