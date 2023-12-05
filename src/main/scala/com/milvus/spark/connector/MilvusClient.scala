package com.milvus.spark.connector

import io.milvus.client.MilvusServiceClient
import io.milvus.param.collection.GetCollectionStatisticsParam
import io.milvus.param.{ConnectParam, LogLevel, R}
import io.milvus.response.GetCollStatResponseWrapper

/*
This is a wrapper class for the Milvus Java SDK.
It lets readers of individual partitions use their
own client in order to perform operations.
Any RPC error handling should be performed here, and
TODO: either a more user-friendly error should be returned to the caller
(or handled here)
 */

val SuccessCode = R.Status.Success.getCode

class MilvusClient(host: String, port:Int) {
  private lazy val client = {
    val milvusClient = new MilvusServiceClient(ConnectParam.newBuilder.withHost(host).withPort(port).build)
    milvusClient.setLogLevel(LogLevel.Error)
    milvusClient
  }
  def getCollectionStatistics(collectionName: String) = {
    val param = GetCollectionStatisticsParam.newBuilder.withCollectionName(collectionName).build
    val response = client.getCollectionStatistics(param)
    if (response.getStatus != SuccessCode) {
      println(response.getMessage)
      -1
    }
    val wrapper = new GetCollStatResponseWrapper(response.getData)
    wrapper.getRowCount
  }
}

object MilvusClient {
  private val DEFAULT_HOST = "localhost"
  private val DEFAULT_PORT = 19530
  def apply() = {
    new MilvusClient(DEFAULT_HOST, DEFAULT_PORT)
  }

  def apply(host: String, port: Int) = {
    new MilvusClient(host, port)
  }
}
