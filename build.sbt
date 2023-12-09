ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.1"


lazy val root = (project in file("."))
  .settings(
    name := "Spark-Milvus",
    idePackagePrefix := Some("org.bdad.spark-milvus")
  )

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

// (optional) If you need scalapb/scalapb.proto or anything from
// google/protobuf/*.proto
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.1.2",
  "org.apache.spark" % "spark-sql_2.12" % "3.1.2",
  "io.milvus" % "milvus-sdk-java" % "2.3.3",
  "io.grpc" % "grpc-netty" % "1.46.0",
//  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
//  , // TODO: remove
//  "org.apache.httpcomponents" % "httpcore" % "4.4.16", // TODO: remove,
//  "com.google.inject" % "guice" % "4.2.2"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.7.1",
  "io.netty" % "netty-all" % "4.1.72.Final",
  "io.netty" % "netty-buffer" % "4.1.72.Final",
//  "com.google.guava" % "guava" % "20.0",
//  "com.google.inject" % "guice" % "4.2.2"
)

excludeDependencies ++= Seq(
    ExclusionRule(organization = "io.netty.buffer")
)

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _                        => MergeStrategy.first
}

