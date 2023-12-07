package com.milvus.spark.connector

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

case class MilvusPartition(index: Int, startKey: Long, endKey: Long, collectionSchema: StructType) extends Partition with InputPartition{
}
