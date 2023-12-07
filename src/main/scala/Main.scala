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
        .option("spark.milvus.numPartitions", 3)
        .load()
    println("Num Partitions: " + df.rdd.getNumPartitions)
    df.show()
    println(df.count())
  }

  def test3(): Unit = {
    val spark = SparkSession.builder()
        .appName("Milvus Loader")
        .master("local[4]")
        .getOrCreate()

    val df = spark.read
        .format("com.milvus.spark.connector.MilvusTableProvider")
        .option("spark.milvus.collectionName", "search_article_in_medium")
        .option("spark.milvus.numPartitions", 3)
        .option("spark.milvus.predicateFilter", "publication==\"The Startup\"")
        .load()
    println("Num Partitions: " + df.rdd.getNumPartitions)
    df.show()
    println(df.count())
    df.rdd.mapPartitions(iter => Iterator(iter.size)).collect().zipWithIndex.foreach { case (count, index) =>
      println(s"Partition $index has $count records.")
    }
  }

  def test4(): Unit = {
    val spark = SparkSession.builder()
        .appName("Milvus Loader")
        .master("local[4]")
        .getOrCreate()

    val df = spark.read
        .format("com.milvus.spark.connector.MilvusTableProvider")
        .option("spark.milvus.collectionName", "search_article_in_medium")
        .option("spark.milvus.numPartitions", 3)
        .option("spark.milvus.predicateFilter", "publication==\"The Startup\"")
        .option("spark.milvus.fields", "id,reading_time, publication,claps  ")
        .load()
    println("Num Partitions: " + df.rdd.getNumPartitions)
    df.show()
    println(df.count())
    df.rdd.mapPartitions(iter => Iterator(iter.size)).collect().zipWithIndex.foreach { case (count, index) =>
      println(s"Partition $index has $count records.")
    }
  }
  def main(args: Array[String]): Unit = {
    test4()
  }
}
