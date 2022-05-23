package com.beilin.Spark_Guide.chapter_2

import org.apache.spark.sql.{DataFrame, SparkSession}

object _02DataFrame {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local").appName("DataFrame")
      .getOrCreate()

    val flightData2015 = spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv("E:\\My_Spark\\BeiLin_Spark\\data\\flight-data\\csv\\2015-summary.csv")

    flightData2015.take(3)
    flightData2015.sort("count").explain()

    // 默认shuffle会输出200个shuffle分区，设成5个
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    flightData2015.sort("count").take(2)
    // 把DataFrame注册成数据表或视图（临时表）
    flightData2015.createOrReplaceTempView("flight_data_2015")
    // 使用SparkSQL查询
    val sqlWay = spark.sql(
      """
          select DEST_COUNTRY_NAME,count(1)
          from flight_data_2015
          group by DEST_COUNTRY_NAME
        """)
    val dataFrameWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()
    // 查看执行计划
    sqlWay.explain()
    dataFrameWay.explain()

    spark.sql("select max(count) from flight_data_2015").take(1)
    //
      import org.apache.spark.sql.functions.max
     flightData2015.select(max("count")).take(1)


    val maxSql = spark.sql("""
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5
    """)
    maxSql.show()

    import org.apache.spark.sql.functions.desc
    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()
      //.explain()
  }
}
