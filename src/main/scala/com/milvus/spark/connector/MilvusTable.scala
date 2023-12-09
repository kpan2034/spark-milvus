package com.milvus.spark.connector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.io.Source
import scala.jdk.CollectionConverters._

case class MilvusTable(conf: MilvusConnectorConf) extends Table with SupportsRead {
  override def name(): String = "a milvus collection appears"

  private lazy val collectionSchema: StructType = {
    getSchema
  }

  private lazy val client = {
    MilvusClient(conf.uri, conf.port, conf.token)
  }

  def getSchema: StructType = {
    val sc = StructType(client.describeCollection(conf.collectionName).filter(field => conf.fields.contains(field.name)))
    client.close()
    sc
  }

  override def schema(): StructType = {
   collectionSchema
  }

  // todo: look into what capabilities are
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val scanBuilder = MilvusScanBuilder(conf, collectionSchema)
    scanBuilder
  }
}
