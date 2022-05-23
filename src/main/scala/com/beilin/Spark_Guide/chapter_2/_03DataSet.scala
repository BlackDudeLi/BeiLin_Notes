package com.beilin.Spark_Guide.chapter_2

import org.apache.spark.sql.SparkSession

object _03DataSet {
  def main(args: Array[String]): Unit = {
    case class Flight(
                       DEST_COUNTRY_NAME:String,
                       ORIGIN_COUNTRY_NAME:String,
                       count:BigInt
                     )
    val spark:SparkSession = SparkSession.builder().appName("DataSet").master("local")
      .getOrCreate()
    import spark.implicits._
//    val flightsDF = spark.read.parquet("E:\\My_Spark\\BeiLin_Spark\\data\\flight-data\\parquet\\2010-summary.parquet")
//    val flights = flightsDF.as[Flight]
//    flights
//      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
//      .map(flight_row => flight_row)
//      .take(5)
//    flights
//      .take(5)
//      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
//      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
    val staticDataFrame = spark.read.format("csv")
      .option("header","true")
      .option("inferSchame","true")
      .csv("E:\\My_Spark\\BeiLin_Spark\\data\\retail-data\\by-day\\*.csv")

    import org.apache.spark.sql.functions.{window, column, desc, col}
    staticDataFrame.createGlobalTempView("retail_data")
    val staticSchema = staticDataFrame.schema
    staticSchema.printTreeString()
    staticDataFrame.selectExpr(
      "CustomerID",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate"
    ).groupBy(
          col("CustomerId"), window(col("InvoiceDate"), "1 day"))
        .sum("total_cost")
        .show(5)



    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger",1)
      .format("csv")
      .option("header","true")
      .load("E:\\My_Spark\\BeiLin_Spark\\data\\retail-data\\by-day\\*.csv")

    streamingDataFrame.isStreaming

    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        $"CustomerId", window($"InvoiceDate", "1 day"))
      .sum("total_cost")

    purchaseByCustomerPerHour.writeStream
      .format("memory") // memory 代表将表存入内存
      .queryName("customer_purchases") // 存入内存的表的名称
      .outputMode("complete") // complete 表示保存表中所有记录
      .start()

    spark.sql("""
        SELECT *
        FROM customer_purchases
        ORDER BY `sum(total_cost)` DESC
    """)
      .show(5)

  }
}
