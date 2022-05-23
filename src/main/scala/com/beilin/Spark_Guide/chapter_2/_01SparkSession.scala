package com.beilin.Spark_Guide.chapter_2

import org.apache.spark.sql.SparkSession

object _01SparkSession {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local").appName("SparkSQL")
      .getOrCreate()
    // page45
    val myRange = spark.range(1000).toDF("number")
    val divisBy2 = myRange.where("number % 2 = 0 ")
    divisBy2.count()
  }

}
