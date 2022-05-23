package com.beilin.Spark_Guide.chapter_1

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionCreateMethod {
  def main(args: Array[String]): Unit = {
    // 第一种方法：直接使用SparkSession的builder构建器
    val spark: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local")
      .config("","")
      .config("", "")
      .getOrCreate()

    val df: DataFrame = spark.read.json("E:\\My_Spark\\BeiLin_Spark\\data\\a.json")
    df.show()

    //第二种方法：使用SparkConf绑定配置信息
    /*
    val conf = new SparkConf().setMaster("local").setAppName("test").set("a","true")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark.read.json("data/a.json").show()
    */

    // 第三种方法：获取连接hive的SparkSession对象
    /*
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("test")
      .enableHiveSupport()
      .getOrCreate()
    spark.table("").show()

    spark.stop()
    */
  }
}
