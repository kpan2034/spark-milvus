package org.bdad.sparkmilvus

import com.milvus.spark.connector.MilvusClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ReuseSubquery.conf
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

  def measureSdkTime(): Unit = {
    val client: MilvusClient = {
      MilvusClient("https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com", 19530, "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
    }
    val predicateFilter = "publication==\"The Startup\""
    val collectionName = "search_article_in_medium"
    val primaryKeys: Array[Long] = client.queryCollection(
      collectionName = collectionName,
      fieldsToReturn = List("id"),
      query = predicateFilter
    ).map(_.get("id").asInstanceOf[Long]).toArray
    val sortedPrimaryKeys = primaryKeys.sorted
    val startKey = sortedPrimaryKeys.apply(0)
    val endKey = sortedPrimaryKeys.length - 1
    val filter = "id>=" + startKey.toString + " and id<=" + endKey.toString + " and " + predicateFilter // TODO: change id to PK

    val reqFields: List[String] = List("id","reading_time", "publication","claps")
    val records = {
      client.queryCollection(collectionName, reqFields, query = filter)
    }
  }

  def testManagedMilvus(): Unit = {
    val spark = SparkSession.builder()
      .appName("Milvus Loader")
      .master("local[*]")
      .getOrCreate()

//    val df = spark.read
//      .format("com.milvus.spark.connector.MilvusTableProvider")
//      .option("spark.milvus.uri", "https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com")
//      .option("spark.milvus.token", "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
//      .option("spark.milvus.collectionName", "search_article_in_medium")
//      .option("spark.milvus.numPartitions", 6)
////      .option("spark.milvus.predicateFilter", "publication==\"The Startup\"")
//      .option("spark.milvus.fields", "id,reading_time, publication,claps  ")
//      .load()
    val df1 = spark.read
      .format("com.milvus.spark.connector.MilvusTableProvider")
      .option("spark.milvus.uri", "https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com")
      .option("spark.milvus.token", "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
      .option("spark.milvus.collectionName", "search_article_in_medium")
      .option("spark.milvus.numPartitions", 6)
//            .option("spark.milvus.predicateFilter", "publication==\"The Startup\"")
      .option("spark.milvus.fields", "id,reading_time, publication,claps  ")
      .load()
      .where("claps >= 10000")
    println(spark.time(df1
        .show(500, false)
      ))
//      println(spark.time(spark.read
//        .format("com.milvus.spark.connector.MilvusTableProvider")
//        .option("spark.milvus.uri", "https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com")
//        .option("spark.milvus.token", "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
//        .option("spark.milvus.collectionName", "search_article_in_medium")
//        .option("spark.milvus.numPartitions", 3)
//        .option("spark.milvus.predicateFilter", "publication==\"The Startup\"")
//        .option("spark.milvus.fields", "id,reading_time, publication,claps  ")
//        .load()))
//    println("Num Partitions: " + df.rdd.getNumPartitions)
////    df.show()
//    println(df.count())
//    df.rdd.mapPartitions(iter => Iterator(iter.size)).collect().zipWithIndex.foreach { case (count, index) =>
//      println(s"Partition $index has $count records.")
//    }
  }

  def testManagedMilvus2(): Unit = {
    val spark = SparkSession.builder()
      .appName("Milvus Loader")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.read
      .format("com.milvus.spark.connector.MilvusTableProvider")
      .option("spark.milvus.uri", "https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com")
      .option("spark.milvus.token", "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
      .option("spark.milvus.collectionName", "search_article_in_medium")
      .option("spark.milvus.numPartitions", 2)
      .option("spark.milvus.predicateFilter", "claps >= 10000")
      .option("spark.milvus.fields", "id,reading_time, publication,claps  ")
      .load()
//      .where("claps >= 10000")
    println(spark.time(df1
      .show(500, false)
    ))
  }
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("Milvus Loader")
//      .master("local[4]")
//      .getOrCreate()
//    spark.time(measureSdkTime())
    testManagedMilvus2()
  }
}
