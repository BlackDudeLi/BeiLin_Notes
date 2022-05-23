package com.beilin.Spark_Guide.chapter_1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object AppAccessDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySpark").setMaster("local")
    val sc = new SparkContext(conf)
    // Spark 2.0 以后的SparkSession对象
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val df:DataFrame = spark.read.json("E:\\My_Spark\\BeiLin_Spark\\data\\emp.json")
    df.show()
    sc.stop()
  }

}
