package com.csvloader.spark.connector

import org.apache.spark.Partition
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScanBuilder
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider, StreamSinkProvider}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.jdk.CollectionConverters._
import scala.util.parsing.json.JSON.headOptionTailToFunList
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source

class CsvTableProvider extends TableProvider with DataSourceRegister {
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }

  override def shortName(): String = "csv-loader"

  def getTable(options: CaseInsensitiveStringMap): CsvTable = {
    val session = SparkSession.active
    val sparkConf = session.sparkContext.getConf
    val scalaOptions = options.asScala

    val filename: String = scalaOptions.getOrElse("filename", throw new IllegalArgumentException("No filename specified in options for CSV Loader"))
    CsvTable(filename)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(options).schema()
  }
}

case class CsvTable(filename: String) extends Table with SupportsRead {
  override def name(): String = "a csv file appears"

  private lazy val fileSchema = {
    getSchema
  }

  def getSchema: StructType = {
    // First open the file
    val bufferedSource = Source.fromFile(filename)
    println("okay made it here")
    val headers = bufferedSource.getLines().take(1).mkString.split(",").toSeq
    println("okay made it here too")
    println(headers)
    bufferedSource.close()
    // Just read everything in as a String - do type casts manually
    val customSchema : StructType = StructType(headers.map(x => StructField(x, StringType, nullable=true)))
    customSchema
  }

  override def schema(): StructType = {
    fileSchema
  }

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    CsvScanBuilder(filename, fileSchema)
  }
}

case class CsvScanBuilder(filename: String, fileSchema: StructType) extends ScanBuilder {
  override def build(): Scan = {
    CsvScan(filename, fileSchema)
  }

  private var readSchema: StructType = {
    fileSchema
  }
}

case class CsvScan(filename: String, fileSchema: StructType) extends Scan {
  override def readSchema(): StructType = {
    fileSchema
  }

  override def toBatch():Batch = {
    CsvBatch(filename, fileSchema)
  }
}

case class CsvBatch(filename: String, fileSchema: StructType) extends Batch {
  private lazy val inputPartitions : Seq[CsvPartition] = {
    // TODO figure out file size
//    val maxLen: Int = 10
//    val stepSize: Int = 10
//    // Now break into partitions of fixed length
//    for ((start, idx) <- (0 to maxLen by stepSize).zip(0 to maxLen/stepSize))
//      yield CsvPartition(idx, start, stepSize, filename, fileSchema)
    Seq(CsvPartition(0, 0, 10, filename, fileSchema))
  }

  override def planInputPartitions(): Array[InputPartition] = {
    inputPartitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    CsvScanPartitionReaderFactory(filename, fileSchema)
  }

}

case class CsvScanPartitionReaderFactory(filename: String, fileSchema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    CsvScanPartitionReader(partition.asInstanceOf[CsvPartition])
  }
}

case class CsvScanPartitionReader(partition: CsvPartition) extends PartitionReader[InternalRow] {
  private var lastRow: InternalRow = _

  private val bufferedSource= Source.fromFile(partition.filename)
  private val csvIterator = bufferedSource.getLines.drop(1)
  private var count: Int = 0

  override def next(): Boolean = {
    count += 1
    println(partition.index + " " + count)
    if (csvIterator.hasNext){
//      csvIterator.next()
      val cols = csvIterator.next().split(",")
      lastRow = InternalRow(UTF8String.fromString(cols(0)), UTF8String.fromString(cols(1)))
      println(lastRow)
      println(csvIterator)
      println(bufferedSource)
      true
    }
    else {
      false
    }
  }
  import org.apache.spark.unsafe.types.UTF8String

  override def get(): InternalRow = {
    lastRow
//    InternalRow(UTF8String.fromString("hello"), UTF8String.fromString("bye"))
  }

  override def close(): Unit = {
//    bufferedSource.close()
  }
}

case class CsvPartition(index: Int, start: Int, partitionLength: Int, filename: String, fileSchema: StructType) extends Partition with InputPartition {
}

