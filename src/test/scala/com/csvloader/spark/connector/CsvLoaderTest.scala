package com.csvloader.spark.connector

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class CsvLoaderTest extends AnyFlatSpec{
  val filename = "test.csv"

  "CSVLoader" should "be able to load a CSV file" in {
    val spark = SparkSession.builder()
      .appName("Milvus Loader")
      .master("local[4]")
      .getOrCreate()

    //    val df = ss.read.csv()

    val df = spark.read
      .format("com.csvloader.spark.connector.CsvTableProvider")
      .option("filename", filename)
      .load()

    println("Num Partitions: " + df.rdd.getNumPartitions)
    df.show()
  }
}
