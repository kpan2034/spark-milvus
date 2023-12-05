package com.milvus.spark.connector

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

case class MilvusScan(conf: MilvusConnectorConf, collectionSchema: StructType) extends Scan{
  override def readSchema(): StructType = {
    collectionSchema
  }

  override def toBatch(): Batch = {
    MilvusBatch(conf, collectionSchema)
  }
}
