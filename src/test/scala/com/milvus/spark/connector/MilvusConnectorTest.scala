package com.milvus.spark.connector

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class MilvusConnectorTest extends AnyFlatSpec{
  val HOST: String = "localhost"
  val PORT: Int = 19530
  val COLLECTION_NAME: String = "search_article_in_medium"
  val COLLECTION_SIZE: Int = 5978

  "MilvusConnector" should "be able to read from Milvus" in {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Milvus Loader")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("com.milvus.spark.connector.MilvusTableProvider")
      .option("spark.milvus.collectionName", COLLECTION_NAME)
      .load()

    assert(df.rdd.getNumPartitions == 1)
    df.show()
    assert(df.count() == COLLECTION_SIZE)
  }
}
