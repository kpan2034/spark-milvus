package com.milvus.spark.connector

import org.apache.spark.internal.Logging

import scala.collection.mutable
/*
This class provides a conf object containing
any configuration options needed to communicate with Milvus.
 */
case class MilvusConnectorConf(
                              uri:String = MilvusConnectorConf.URIParam.default,
                              port:Int = MilvusConnectorConf.PortParam.default,
                              token:String = MilvusConnectorConf.TokenParam.default,
                              collectionName:String = MilvusConnectorConf.CollectionNameParam.default,
                              numPartitions: Int = MilvusConnectorConf.NumPartitionsParam.default,
                              predicateFilter: String = MilvusConnectorConf.PredicateFilterParam.default,
                              fields: Array[String] = Array[String]()
                              ) {
}

// Used to help easily define a ConfigParameter.
// Ref: https://github.com/datastax/spark-cassandra-connector/blob/f6476a1b8f71ff83a89c8d74cdc94f93b5a2cc8a/connector/src/main/scala/com/datastax/spark/connector/util/ConfigParameter.scala#L10
object MilvusConnectorConf {
  private val URIParam = ConfigParameter[String](
    name = "spark.milvus.uri",
    default = "http://127.0.0.1",
    description = "Milvus URI"
  )

  private val PortParam = ConfigParameter[Int](
    name = "spark.milvus.port",
    default = 19530,
    description = "Milvus Port"
  )

  private val TokenParam = ConfigParameter[String] (
    name = "spark.milvus.token",
    default = "",
    description = "Token for managed milvus instances"
  )

  private val CollectionNameParam = ConfigParameter[String](
    name = "spark.milvus.collectionname",
    default = "default",
    description = "Milvus Collection Name"
  )

  private val NumPartitionsParam = ConfigParameter[Int](
    name = "spark.milvus.numpartitions",
    default = 4,
    description = "Number of DataFrame partitions"
  )

  private val PredicateFilterParam = ConfigParameter[String](
    name = "spark.milvus.predicatefilter",
    default = "id >= 0",
    description = "Predicate for filtering data in milvus"
  )

  private val FieldsParam = ConfigParameter[String](
    name = "spark.milvus.fields",
    default = "",
    description = "Required fields to be queried"
  )
  def parseOptions(options: Map[String, String]): MilvusConnectorConf = {
    val uri: String = options.getOrElse(URIParam.name, URIParam.default)
    val port: Int = options.getOrElse(PortParam.name, PortParam.default).asInstanceOf[Int]
    val token: String = options.getOrElse(TokenParam.name, TokenParam.default)
    val collectionName: String = options.getOrElse(CollectionNameParam.name, CollectionNameParam.default)
    val numPartitions: Int = options.getOrElse(NumPartitionsParam.name, NumPartitionsParam.default).asInstanceOf[String].toInt
    val predicateFilterParam: String = options.getOrElse(PredicateFilterParam.name, PredicateFilterParam.default)
    val fields: Array[String] = options.getOrElse(FieldsParam.name, FieldsParam.default).split(",").map(_.trim)
    MilvusConnectorConf(uri, port, token, collectionName, numPartitions, predicateFilterParam, fields)
  }
}
