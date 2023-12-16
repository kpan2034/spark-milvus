package org.bdad.sparkmilvus
import com.milvus.spark.connector.MilvusClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.mutable.Seq
import scala.jdk.CollectionConverters._
import io.milvus.response.QueryResultsWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
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

  //this function is to use the same processing for the milvus java sdk as used to create the dataframe's internal rows
  //ensures standardized time comparisons
  def mapRowRecordToInternalRow(client: MilvusClient, record: QueryResultsWrapper.RowRecord, collectionSchema: StructType): _root_.org.apache.spark.sql.catalyst.InternalRow = {
    val FloatVectorType = DataTypes.createArrayType(DataTypes.DoubleType)
    val tmp = collectionSchema.map(field => field.dataType match {
      case DataTypes.StringType => UTF8String.fromString(record.get(field.name).asInstanceOf[String])
      case FloatVectorType => {
        // These are the **** hoops you need to **** jump through in order to get a **** float vector in Spark
        // I'm not sure if this is the best way to do it, but it works
        val floatVectorAsAbstractList = record.get(field.name).asInstanceOf[java.util.AbstractList[Float]]
        val floatVectorAsScalaArray = floatVectorAsAbstractList.asScala.toArray
        val arrayData = ArrayData.toArrayData(floatVectorAsScalaArray)
        arrayData
        // ArrayData.toArrayData(record.get(field.name).asInstanceOf[java.util.AbstractList[Float]].asScala.toArray)
      }
      case _ => record.get(field.name)
    }).toArray
    //    val tmp2 = transformStrings(tmp)
    InternalRow(tmp: _*)
    //    InternalRow(record.getFieldValues.asScala.toArray) // Can't do this since map order is not guaranteed!
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

  def measureSdkTimeBatched(): Unit = {
    val client: MilvusClient = {
      MilvusClient("https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com", 19530, "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
    }
//    val predicateFilter = "publication==\"The Startup\""
    val predicateFilter = "id >= 0"
    val collectionName = "search_article_in_medium"
    val primaryKeys: Array[Long] = client.queryCollection(
      collectionName = collectionName,
      fieldsToReturn = List("id"),
      query = predicateFilter
    ).map(_.get("id").asInstanceOf[Long]).toArray
    val sortedPrimaryKeys = primaryKeys.sorted
    var mutSeq: mutable.Seq[InternalRow] = mutable.Seq()

    val collectionSchema = StructType(client.describeCollection("search_article_in_medium")
      .filter(field => List("id", "reading_time", "publication", "claps").contains(field.name)))

    val reqFields: List[String] = List("id", "reading_time", "publication", "claps")
    val NUM_CALLS = 3
    for (i <- 1 to NUM_CALLS) {
      val startKey = sortedPrimaryKeys.apply((i-1) * (sortedPrimaryKeys.length / NUM_CALLS))
      val endKey = sortedPrimaryKeys.apply(i * (sortedPrimaryKeys.length / NUM_CALLS) - 1)
      val filter = "id>=" + startKey.toString + " and id<=" + endKey.toString + " and " + predicateFilter // TODO: change id to PK

      val records = {
        client.queryCollection(collectionName, reqFields, query = filter)
      }
      records.foreach(record => mutSeq = mutSeq :+ mapRowRecordToInternalRow(client, record, collectionSchema))
    }
    mutSeq.foreach(println)
    println(mutSeq.size)
  }

  def measureSdkTimeWithFilter(): Unit = {
    val client: MilvusClient = {
      MilvusClient("https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com", 19530, "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
    }
    val predicateFilter = "claps >= 10000"
    val collectionName = "search_article_in_medium"
    val primaryKeys: Array[Long] = client.queryCollection(
      collectionName = collectionName,
      fieldsToReturn = List("id"),
      query = predicateFilter
    ).map(_.get("id").asInstanceOf[Long]).toArray
    val sortedPrimaryKeys = primaryKeys.sorted
    val startKey = sortedPrimaryKeys.apply(0)
    val endKey = sortedPrimaryKeys.apply(sortedPrimaryKeys.length - 1)
    val filter = "id>=" + startKey.toString + " and id<=" + endKey.toString + " and " + predicateFilter // TODO: change id to PK
    println(filter)
    val reqFields: List[String] = List("id", "reading_time", "publication", "claps")
    val records = client.queryCollection(collectionName, reqFields, query = filter)
    val iterator = records.iterator
    var mutSeq: mutable.Seq[InternalRow] = mutable.Seq()

    val collectionSchema = StructType(client.describeCollection("search_article_in_medium")
      .filter(field => List("id", "reading_time", "publication", "claps").contains(field.name)))
    while (iterator.hasNext) {
      mutSeq = mutSeq :+ mapRowRecordToInternalRow(client, iterator.next(), collectionSchema)
    }
    mutSeq.foreach(println)
  }
  def testManagedMilvusWithVectorCol(): Unit = {
    val spark = SparkSession.builder()
      .appName("Milvus Loader")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.read
      .format("com.milvus.spark.connector.MilvusTableProvider")
      .option("spark.milvus.uri", "https://in03-4deca087036f63f.api.gcp-us-west1.zillizcloud.com")
      .option("spark.milvus.token", "33a6ca21413fd126785a65baa24f2f09afd4976ab61004a60429ebb43d66e5fb6b19d80effac2f93d3a1f5a60d06c0121ff50010")
      .option("spark.milvus.collectionName", "search_article_in_medium")
      .option("spark.milvus.numPartitions", 6)
//      .option("spark.milvus.predicateFilter", "publication==\"The Startup\"")
      .option("spark.milvus.predicateFilter", "claps >= 5005")
      .option("spark.milvus.fields", "id, reading_time, publication, claps, link, responses, title_vector")
      .load()

      println(spark.time(df1
        .show(454404, false)
      ))
    println(df1.count())
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
    //to test just the sdk, spark session is just to use the same timer
    val spark = SparkSession.builder()
      .appName("Milvus Loader")
      .master("local[*]")
      .getOrCreate()
    spark.time(measureSdkTimeBatched())

    //to test the spark-milvus connector with partitioning
    testManagedMilvusWithVectorCol()
  }
}
