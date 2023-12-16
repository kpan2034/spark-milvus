package com.milvus.spark.connector

import org.scalatest.flatspec.AnyFlatSpec

class MilvusClientTest extends AnyFlatSpec{
  val HOST: String = "localhost"
  val PORT: Int = 19530
  val COLLECTION_NAME: String = "search_article_in_medium"

  "MilvusClient" should "be able to connect to Milvus" in {
    val milvusClient = MilvusClient(HOST, PORT)
    assert(milvusClient != null)
  }

  it should "be able to get rows in a collection" in {
    val milvusClient = MilvusClient(HOST, PORT)
    val rowCount = milvusClient.getCollectionRowCount(COLLECTION_NAME)
    assert(rowCount > 0)
  }

  it should "be able to get collection info" in {
    val milvusClient = MilvusClient(HOST, PORT)
    val collectionInfo = milvusClient.describeCollection(COLLECTION_NAME)
    assert(collectionInfo != null)
  }

  it should "be able to query a collection" in {
    val milvusClient = MilvusClient(HOST, PORT)
    val fieldsToReturn = List("id", "title", "title_vector", "link", "reading_time", "publication", "claps", "responses")
    val queryResults = milvusClient.queryCollection(COLLECTION_NAME, fieldsToReturn)
    val elem = queryResults.iterator.next()
    fieldsToReturn.foreach(field => println((elem.get(field)).getClass))
    assert(queryResults != null)
  }
}
