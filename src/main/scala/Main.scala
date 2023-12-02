package org.bdad.sparkmilvus

import com.alibaba.fastjson.JSONObject
import io.milvus.client.MilvusServiceClient
import io.milvus.grpc._
import io.milvus.param._
import io.milvus.param.collection._
import io.milvus.param.dml._
import io.milvus.param.index._
import io.milvus.response._
import scala.collection.JavaConverters._


object Main2{
  private val COLLECTION_NAME = "java_sdk_example_simple"
  private val ID_FIELD = "book_id"
  private val VECTOR_FIELD = "book_intro"
  private val TITLE_FIELD = "book_title"
  private val VECTOR_DIM = 4

  def main2(args: Array[String]): Unit = {
    // Connect to Milvus server. Replace the "localhost" and port with your Milvus server address.
    val milvusClient = new MilvusServiceClient(ConnectParam.newBuilder.withHost("localhost").withPort(19530).build)
    // set log level, only show errors
    milvusClient.setLogLevel(LogLevel.Error)
    // Define fields
    val fieldsSchema = java.util.Arrays.asList(FieldType.newBuilder.withName(ID_FIELD).withDataType(DataType.Int64).withPrimaryKey(true).withAutoID(false).build, FieldType.newBuilder.withName(VECTOR_FIELD).withDataType(DataType.FloatVector).withDimension(VECTOR_DIM).build, FieldType.newBuilder.withName(TITLE_FIELD).withDataType(DataType.VarChar).withMaxLength(64).build)
    // Create the collection with 3 fields
    var ret = milvusClient.createCollection(CreateCollectionParam.newBuilder.withCollectionName(COLLECTION_NAME).withFieldTypes(fieldsSchema).build)
    if (ret.getStatus != R.Status.Success.getCode) throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage)
    // Specify an index type on the vector field.
    ret = milvusClient.createIndex(CreateIndexParam.newBuilder.withCollectionName(COLLECTION_NAME).withFieldName(VECTOR_FIELD).withIndexType(IndexType.FLAT).withMetricType(MetricType.L2).build)
    if (ret.getStatus != R.Status.Success.getCode) throw new RuntimeException("Failed to create index on vector field! Error: " + ret.getMessage)
    // Specify an index type on the varchar field.
    ret = milvusClient.createIndex(CreateIndexParam.newBuilder.withCollectionName(COLLECTION_NAME).withFieldName(TITLE_FIELD).withIndexType(IndexType.TRIE).build)
    if (ret.getStatus != R.Status.Success.getCode) throw new RuntimeException("Failed to create index on varchar field! Error: " + ret.getMessage)
    // Call loadCollection() to enable automatically loading data into memory for searching
    milvusClient.loadCollection(LoadCollectionParam.newBuilder.withCollectionName(COLLECTION_NAME).build)
    println("Collection created")
    // Insert 10 records into the collection
    val rows = new java.util.ArrayList[JSONObject]
    for (i <- 1L to 10) {
      val row = new JSONObject
      row.put(ID_FIELD, i)
      val vector = java.util.Arrays.asList(i.toFloat, i.toFloat, i.toFloat, i.toFloat)
      row.put(VECTOR_FIELD, vector)
      row.put(TITLE_FIELD, "Tom and Jerry " + i)
      rows.add(row)
    }
    val insertRet = milvusClient.insert(InsertParam.newBuilder.withCollectionName(COLLECTION_NAME).withRows(rows).build)
    if (insertRet.getStatus!=R.Status.Success.getCode) throw new RuntimeException("Failed to insert! Error: " + insertRet.getMessage)
    // Call flush to make sure the inserted records are consumed by Milvus server, so that the records
    // be searchable immediately. Just a special action in this example.
    // In practice, you don't need to call flush() frequently.
    milvusClient.flush(FlushParam.newBuilder.addCollectionName(COLLECTION_NAME).build)
    println("10 entities inserted")
    // Construct a vector to search top5 similar records, return the book title for us.
    // This vector is equal to the No.3 record, we suppose the No.3 record is the most similar.
    val vector = java.util.Arrays.asList(3.0f, 3.0f, 3.0f, 3.0f)
    val searchRet = milvusClient.search(SearchParam.newBuilder.withCollectionName(COLLECTION_NAME).withMetricType(MetricType.L2).withTopK(5).withVectors(java.util.Arrays.asList(vector)).withVectorFieldName(VECTOR_FIELD).withParams("{}").addOutField(TITLE_FIELD).build)
    if (searchRet.getStatus!=R.Status.Success.getCode) throw new RuntimeException("Failed to search! Error: " + searchRet.getMessage)
    // The search() allows multiple target vectors to search in a batch.
    // Here we only input one vector to search, get the result of No.0 vector to print out
    val resultsWrapper = new SearchResultsWrapper(searchRet.getData.getResults)
    val scores = resultsWrapper.getIDScore(0)
    println("The result of No.0 target vector:")
    import scala.collection.JavaConversions._
    for (score <- scores) {
      println(score)
    }
    // drop the collection if you don't need the collection anymore
    milvusClient.dropCollection(DropCollectionParam.newBuilder.withCollectionName(COLLECTION_NAME).build)
  }
}