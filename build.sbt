ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.1"

lazy val root = (project in file("."))
  .settings(
    name := "Spark-Milvus",
    idePackagePrefix := Some("org.bdad.spark-milvus")
  )

libraryDependencies += "io.milvus" % "milvus-sdk-java" % "2.3.3"
