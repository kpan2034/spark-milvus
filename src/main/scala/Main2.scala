package org.bdad.sparkmilvus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main{
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Milvus Loader")
      .master("local[4]")
      .getOrCreate()

//    val df = ss.read.csv()

    val df = spark.read
      .format("com.milvus.spark.connector.MilvusTableProvider")
      .option("spark.milvus.collectionName", "search_article_in_medium")
      .load()
    println("Num Partitions: " + df.rdd.getNumPartitions)
    df.show()
    println(df.count())
  }
}
