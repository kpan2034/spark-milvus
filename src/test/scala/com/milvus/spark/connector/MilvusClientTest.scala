package com.milvus.spark.connector

import org.scalatest.flatspec.AnyFlatSpec

class MilvusClientTest extends AnyFlatSpec{
  val HOST: String = "localhost"
  val PORT: Int = 19530
  val collectionName: String = "search_article_in_medium"

  "MilvusClient" should "be able to connect to Milvus" in {
    val milvusClient = MilvusClient(HOST, PORT)
    assert(milvusClient != null)
  }

  it should "be able to get rows in a collection" in {
    val milvusClient = MilvusClient(HOST, PORT)
    val rowCount = milvusClient.getCollectionRowCount(collectionName)
    assert(rowCount > 0)
  }

  it should "be able to get collection info" in {
    val milvusClient = MilvusClient(HOST, PORT)
    val collectionInfo = milvusClient.describeCollection(collectionName)
    assert(collectionInfo != null)
  }
}
