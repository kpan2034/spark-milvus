package com.milvus.spark.connector

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

case class MilvusPartition(index: Int, start: Int, partitionLength: Int) extends Partition with InputPartition{
}
