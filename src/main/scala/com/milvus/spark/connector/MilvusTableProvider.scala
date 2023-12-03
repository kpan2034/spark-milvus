package com.milvus.spark.connector

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class MilvusTableProvider extends TableProvider with DataSourceRegister {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(options).schema()
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }

   private def getTable(options: CaseInsensitiveStringMap): Table = {
    // TODO extract config options from here into a MilvusClientConf object
    MilvusTable()
  }

  override def shortName(): String = "spark-milvus"

}