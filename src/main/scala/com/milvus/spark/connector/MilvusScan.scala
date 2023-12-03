package com.milvus.spark.connector

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

case class MilvusScan(fileSchema: StructType) extends Scan{
  override def readSchema(): StructType = {
    fileSchema
  }

  override def toBatch(): Batch = {
    MilvusBatch(fileSchema)
  }
}
