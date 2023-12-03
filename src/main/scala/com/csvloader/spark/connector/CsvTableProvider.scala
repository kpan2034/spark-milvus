package com.csvloader.spark.connector

import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.io.Source
import scala.jdk.CollectionConverters._

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
    val headers = bufferedSource.getLines().take(1).mkString.split(",").toSeq
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
  private def getFileSize() = {
    val bufferedSource = Source.fromFile(filename)
    val len = bufferedSource.getLines().size - 1
    bufferedSource.close()
    len
  }

  private lazy val inputPartitions : Seq[CsvPartition] = {
    val maxLen: Int = getFileSize()
    val stepSize: Int = 3
    // Now break into partitions of fixed length
    for ((start, idx) <- (0 until maxLen by stepSize).zip(0 to maxLen/stepSize))
      yield CsvPartition(idx, start, stepSize, filename, fileSchema)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    inputPartitions.toArray.foreach(x => print(x + " "))
    println()
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
  private val csvIterator = bufferedSource.getLines.drop(1+partition.start)
  private var processedCount: Int = 0
  private var closed: Boolean = false

  override def next(): Boolean = {
    processedCount += 1
    if (csvIterator.hasNext && processedCount <= partition.partitionLength){
      assert(!closed)
      val cols = csvIterator.next().split(",")
      // convert each of these columns to a UTF8String that can be used to create an internal row
      // if there are null values in the input, cols will have fewer columns than expected
      // so we use padTo() to ensure cols is always of the expected size
      lastRow = InternalRow(cols.map(x => UTF8String.fromString(x)).padTo(partition.fileSchema.length, null):_*)
      true
    }
    else {
      false
    }
  }

  override def get(): InternalRow = {
    assert(!closed)
    lastRow
  }

  override def close(): Unit = {
    if(!closed) {
      closed = true
      println("closing source")
      bufferedSource.close()
    }
  }
}

case class CsvPartition(index: Int, start: Int, partitionLength: Int, filename: String, fileSchema: StructType) extends Partition with InputPartition {
}

