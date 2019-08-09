package com

import org.apache.spark.sql.SparkSession

/**
 * DataFrame API基本操作
 */
object DataSetApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DataSetApp")
        .master("local[2]").getOrCreate()
    //spark如何解析csv文件
    val path = "D:\\BigDataTest\\order_info_utf.csv"
    val df = spark.read.option("header","true").option("inferSchema","true").csv(path)
    df.show()
    spark.stop()
  }

}
