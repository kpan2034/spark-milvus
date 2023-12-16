# Spark-Milvus


This is a Milvus connector library for Spark. It can be used by building the jar from the source code and including it in your application.


## Dependencies

Please ensure you have Spark 3.1.2, Scala 2.11.12 and Java 8 installed. The other dependencies are handled by the library itself.

## Building the jar

```
git clone https://github.com/kpan2034/spark-milvus.git
cd spark-milvus
sbt assembly
```

You can include the jar `spark-milvus_2.12-0.1.0-SNAPSHOT.jar` in the spark submit command for your Spark application.

Example spark-submit command:
```spark-submit --class org.bdad.sparkmilvus.Main --jars jars/milvus-sdk-java-2.3.3.jar target/scala-2.12/spark-milvus_2.12-0.1.0-SNAPSHOT.jar```


## Usage

You can use this connector whereever you use `spark.read`, in the following way:

```
val df = spark.read
    .format("com.milvus.spark.connector.MilvusTableProvider") // specify connector
    .option("spark.milvus.uri", "<HOST URI") // host for your Milvus cluster
    .option("spark.milvus.token", "<TOKEN>") // connectioni token
    .option("spark.milvus.collectionName", "search_article_in_medium") // collection name in Milvus
    .option("spark.milvus.numPartitions", 128) // number of partitions you want to read in
    .option("spark.milvus.predicateFilter", "publication==\"The Startup\"") // any filtering you want to perform at Spark level
    .load()
```