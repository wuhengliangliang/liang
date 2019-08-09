package com

import org.apache.spark.sql.SparkSession

/**
 * DataFrame API基本操作
 */
object DataFrameApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    // 将json文件加载成一个dataframe
    val peopleDF = spark.read.format("json").load("D:\\BigDataTest\\people.json")



    spark.stop()
  }

}
