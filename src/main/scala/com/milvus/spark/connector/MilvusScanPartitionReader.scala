package com.milvus.spark.connector

import io.milvus.response.QueryResultsWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.jdk.CollectionConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}

case class MilvusScanPartitionReader(conf: MilvusConnectorConf, partition: InputPartition) extends PartitionReader[InternalRow]{
  private var lastRow: InternalRow = _
  private var closed: Boolean = false
  private val collectionSchema = partition.asInstanceOf[MilvusPartition].collectionSchema
  private lazy val client: MilvusClient = {
    MilvusClient(conf.host, conf.port)
  }

  // I'm not sure if you need this, or if you can just maintain a reference to the iterator
  // GC is probably smart enough to know not to clean up the underlying data
  val startKey: Long = partition.asInstanceOf[MilvusPartition].startKey
  val endKey: Long = partition.asInstanceOf[MilvusPartition].endKey
  private lazy val filter = "id>=" + startKey.toString + " and id<=" + endKey.toString + " and " + conf.predicateFilter // TODO: change id to PK
  private lazy val reqFields = if (conf.fields.isEmpty) collectionSchema.fieldNames.toList else conf.fields.toList
  private lazy val records = {
    client.queryCollection(conf.collectionName, reqFields, query = filter)
  }

  private lazy val iterator = {
    records.iterator
  }

  private def mapRowRecordToInternalRow(record: QueryResultsWrapper.RowRecord): _root_.org.apache.spark.sql.catalyst.InternalRow = {
    val FloatVectorType = DataTypes.createArrayType(DataTypes.DoubleType)
    val tmp = collectionSchema.map(field => field.dataType match{
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
    InternalRow(tmp:_*)
//    InternalRow(record.getFieldValues.asScala.toArray) // Can't do this since map order is not guaranteed!
  }

//  private def transformStrings(values: Seq[Any]): Seq[Any] = {
//    values.zipWithIndex.map {
//      case (value, index) =>
//        val dataType = collectionSchema.fields(index).dataType
//        dataType match {
//          case _: StringType => value.toString
//          case _ => value
//        }
//    }
//  }

  // Important: need to figure out how to convert data in Milvus to InternalRow correctly
  override def next(): Boolean = {
    if (iterator.hasNext) {
      val nextValue = iterator.next()
      lastRow = mapRowRecordToInternalRow(nextValue)
      true
    } else
    false
  }

  override def get(): InternalRow = {
    lastRow
  }

  override def close(): Unit = {
    if(!closed) {
      closed = true
      client.close()
    }
  }
}
