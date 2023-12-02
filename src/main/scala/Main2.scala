package org.bdad.sparkmilvus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Main{
  def main(args: Array[String]): Unit = {
    // Create SparkSession

    val spark = SparkSession.builder()
      .appName("CSV Loader")
      .master("local[4]")
      .getOrCreate()

//    val df = ss.read.csv()

    val df = spark.read
      .format("com.csvloader.spark.connector.CsvTableProvider")
      .option("filename", "test.csv")
      .load()
//    println(df.rdd.getNumPartitions)
    df.show()
  }
}
