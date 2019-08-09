package com

import org.apache.spark.sql.SparkSession

/**
 * DataFrame中的操作操作
 */
object DataFrameCase {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("D:\\BigDataTest\\data\\student.data")

    //注意：需要导入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //show默认只显示前20条
    studentDF.show
    studentDF.show(30)
    studentDF.show(30, false)


    studentDF.select("name","email").sort("name").show(30,false)
    studentDF.filter("name='' OR name='null'").show()
    studentDF.filter("SUBSTR(name,0,1)='A'").show()
    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
