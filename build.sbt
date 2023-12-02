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
  "io.milvus" % "milvus-sdk-java" % "2.3.3",
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)
