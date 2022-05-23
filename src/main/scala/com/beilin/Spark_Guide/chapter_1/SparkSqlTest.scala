package com.beilin.Spark_Guide.chapter_1


import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local").appName("SparkSQL")
      .getOrCreate()
    import spark.implicits._
    // 读取一个json文件，返回DataFrame对象,可以看作一个二维表
    val df:DataFrame = spark.read.json("E:\\My_Spark\\BeiLin_Spark\\data\\emp.json")
    df.show(10,false)

    val schema:StructType = df.schema
    schema.printTreeString()
    df.printSchema()

    val df1:DataFrame =df.select("empno","ename","job")
    df1.show()

  }

}
