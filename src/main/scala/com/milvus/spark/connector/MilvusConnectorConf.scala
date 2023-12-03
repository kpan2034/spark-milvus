package com.milvus.spark.connector

import org.apache.spark.internal.Logging

/*
This class provides a conf object containing
any configuration options needed to communicate with Milvus.
 */
case class MilvusConnectorConf(
                              host:String = MilvusConnectorConf.HostParam.default,
                              port:Int = MilvusConnectorConf.PortParam.default
                              ) {
}

// Used to help easily define a ConfigParameter.
// Ref: https://github.com/datastax/spark-cassandra-connector/blob/f6476a1b8f71ff83a89c8d74cdc94f93b5a2cc8a/connector/src/main/scala/com/datastax/spark/connector/util/ConfigParameter.scala#L10
object MilvusConnectorConf {
  val HostParam = ConfigParameter[String](
    name = "spark.milvus.host",
    default = "localhost",
    description = "Milvus Host"
  )

  val PortParam = ConfigParameter[Int](
    name = "spark.milvus.port",
    default = 19530,
    description = "Milvus Port"
  )
}
