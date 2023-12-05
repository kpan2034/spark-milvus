package com.milvus.spark.connector

import io.milvus.client.MilvusServiceClient
import io.milvus.grpc.DataType
import io.milvus.param.collection.{DescribeCollectionParam, FieldType, GetCollectionStatisticsParam}
import io.milvus.param.{ConnectParam, LogLevel, R}
import io.milvus.response.{DescCollResponseWrapper, GetCollStatResponseWrapper}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.jdk.CollectionConverters.asScalaBufferConverter

/*
This is a wrapper class for the Milvus Java SDK.
It lets readers of individual partitions use their
own client in order to perform operations.
Any RPC error handling should be performed here, and
todo: either a more user-friendly error should be returned to the caller
(or handled here)
 */


class MilvusClient(host: String, port: Int) {

  private lazy val client: MilvusServiceClient = {
    val milvusClient = new MilvusServiceClient(ConnectParam.newBuilder.withHost(host).withPort(port).build)
    milvusClient.setLogLevel(LogLevel.Error)
    milvusClient
  }

  def describeCollection(collectionName: String): StructType = {
    val param = DescribeCollectionParam.newBuilder.withCollectionName(collectionName).build
    val response = client.describeCollection(param)
    if (response.getStatus != R.Status.Success.getCode) {
      println(response.getMessage)
      -1
    }
    val wrapper = new DescCollResponseWrapper(response.getData)
    val fields = wrapper.getFields.map(MilvusClient.fieldToStructMapper).toArray
    val collectionSchema: StructType = DataTypes.createStructType(fields)
    collectionSchema
  }

  def getCollectionRowCount(collectionName: String): Long = {
    val param = GetCollectionStatisticsParam.newBuilder.withCollectionName(collectionName).build
    val response = client.getCollectionStatistics(param)
    if (response.getStatus != R.Status.Success.getCode) {
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

  def apply(): MilvusClient = {
    MilvusClient(DEFAULT_HOST, DEFAULT_PORT)
  }

  def apply(host: String, port: Int): MilvusClient = {
    MilvusClient(host, port)
  }

  private def fieldToStructMapper(field: FieldType): StructField = {
    val name = field.getName
    val dt = field.getDataType
    val structFieldType = dt match {
      case DataType.Array => DataTypes.createArrayType(mapToScalaType(field.getElementType))
      case _ => mapToScalaType(dt)
      }
    val structField: StructField = DataTypes.createStructField(name, structFieldType, true)
    structField
  }

  private def mapToScalaType(dt: DataType): org.apache.spark.sql.types.DataType = {
    // Ref: https://milvus.io/docs/schema.md#Supported-data-types
    dt match {
      case DataType.Bool => DataTypes.BooleanType
      case DataType.Int8 | DataType.Int16 | DataType.Int32 | DataType.Int64 => DataTypes.IntegerType
      case DataType.Float => DataTypes.FloatType
      case DataType.Double => DataTypes.DoubleType
      case DataType.String | DataType.VarChar => DataTypes.StringType
      // TODO: can convert json to a map then use MapType here
      case DataType.JSON => throw new IllegalArgumentException("JSON type not supported")
      // TODO: figure out how to handle vector data -- Array might be the way to go
      case DataType.BinaryVector => throw new IllegalArgumentException("Binary vector type not supported")
      case DataType.FloatVector | DataType.Float16Vector => throw new IllegalArgumentException("Float vector type not supported")
      case _ => throw new IllegalArgumentException("Unknown data type:" + dt)
    }
  }
}
