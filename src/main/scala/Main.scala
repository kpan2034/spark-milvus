package org.bdad.sparkmilvus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main{
  def test1(): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
        .appName("Milvus Loader")
        .master("local[4]")
        .getOrCreate()

    val df = spark.read
        .format("com.milvus.spark.connector.MilvusTableProvider")
        .option("spark.milvus.collectionName", "search_article_in_medium")
        .load()
    println("Num Partitions: " + df.rdd.getNumPartitions)
    df.show()
    println(df.count())
  }

  def test2(): Unit = {
    val spark = SparkSession.builder()
        .appName("Milvus Loader")
        .master("local[4]")
        .getOrCreate()

    val df = spark.read
        .format("com.milvus.spark.connector.MilvusTableProvider")
        .option("spark.milvus.collectionName", "search_article_in_medium")
        .option("spark.milvus.numPartitions", 2)
        .load()
    println("Num Partitions: " + df.rdd.getNumPartitions)
    df.show()
    println(df.count())
  }
  def main(args: Array[String]): Unit = {
    test2()
  }
}
