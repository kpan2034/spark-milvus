package com.milvus.spark.connector

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

case class MilvusScanBuilder(fileSchema: StructType) extends ScanBuilder{
  override def build(): Scan = {
    MilvusScan(fileSchema)
  }

  private var readSchema: StructType = {
    fileSchema
  }
}
