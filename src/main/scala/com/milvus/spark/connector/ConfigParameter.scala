package com.milvus.spark.connector

class ConfigParameter[T] private (
                                   val name: String,
                                   val default: T,
                                   val description: String) {
  def option(value: Any): Map[String, String] = {
    require(value != null)
    Map(name -> value.toString)
  }
}

object ConfigParameter{
  val staticParameters = scala.collection.mutable.Set.empty[ConfigParameter[_]]
  def names: scala.Seq[String] = staticParameters.map(_.name).toSeq

  def apply[T](name: String, default: T, description: String): ConfigParameter[T] = {
    val param = new ConfigParameter(name, default, description)
    staticParameters.add(param)
    param
  }
}

