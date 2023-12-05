package com.milvus.spark.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}

case class MilvusScanPartitionReader(partition: InputPartition) extends PartitionReader[InternalRow]{
  private var lastRow: InternalRow = _
  private var closed: Boolean = false
  // TODO: implement next and get
  // Important: need to figure out how to convert data in Milvus to InternalRow correctly
  override def next(): Boolean = {
    false
  }

  override def get(): InternalRow = {
    lastRow
  }

  override def close(): Unit = {
    if(!closed) closed = true
  }
}