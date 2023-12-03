package com.milvus.spark.connector

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.io.Source
import scala.jdk.CollectionConverters._

case class MilvusTable() extends Table with SupportsRead {
  override def name(): String = "a csv file appears"

  private lazy val fileSchema = {
    getSchema
  }

  def getSchema: StructType = {
    // First open the file
    val headers = Seq("id", "val")
    val customSchema : StructType = StructType(headers.map(x => StructField(x, StringType, nullable=true)))
    customSchema
  }

  override def schema(): StructType = {
    fileSchema
  }

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    MilvusScanBuilder(fileSchema)
  }
}
