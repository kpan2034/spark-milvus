package com.milvus.spark.connector

import org.apache.spark.internal.Logging

import scala.collection.mutable
/*
This class provides a conf object containing
any configuration options needed to communicate with Milvus.
 */
case class MilvusConnectorConf(
                              host:String = MilvusConnectorConf.HostParam.default,
                              port:Int = MilvusConnectorConf.PortParam.default,
                              collectionName:String = MilvusConnectorConf.CollectionNameParam.default
                              ) {
}

// Used to help easily define a ConfigParameter.
// Ref: https://github.com/datastax/spark-cassandra-connector/blob/f6476a1b8f71ff83a89c8d74cdc94f93b5a2cc8a/connector/src/main/scala/com/datastax/spark/connector/util/ConfigParameter.scala#L10
object MilvusConnectorConf {
  private val HostParam = ConfigParameter[String](
    name = "spark.milvus.host",
    default = "localhost",
    description = "Milvus Host"
  )

  private val PortParam = ConfigParameter[Int](
    name = "spark.milvus.port",
    default = 19530,
    description = "Milvus Port"
  )

  private val CollectionNameParam = ConfigParameter[String](
    name = "spark.milvus.collectionName",
    default = "default",
    description = "Milvus Collection Name"
  )

  def parseOptions(options: Map[String, String]): MilvusConnectorConf = {
    // TODO: add more properties as needed
    val host: String = options.getOrElse(HostParam.name, HostParam.default)
    val port: Int = options.getOrElse(PortParam.name, PortParam.default).asInstanceOf[Int]
    val collectionName: String = options.getOrElse(CollectionNameParam.name, CollectionNameParam.default)
    MilvusConnectorConf(host, port, collectionName)
  }
}
