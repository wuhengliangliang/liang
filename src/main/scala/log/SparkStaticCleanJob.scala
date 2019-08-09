package log

import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * 使用spark完成我们的是数据清洗操作
  *
  */
object SparkStaticCleanJob {
  def main(args: Array[String]):Unit= {
    val spark = SparkSession.builder().appName("SparkStaticCleanJob").master("local[2]").getOrCreate()
    val accessRDD = spark.sparkContext.textFile("D:\\BigDataTest\\data\\access1.log")
//    accessRDD.take(10).foreach(println)
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
     AccessConvertUtil.struct)
//        accessDF.printSchema()
//        accessDF.show(false)
    //coalesce(1)写入一个文件
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
      .save("D:\\BigDataTest\\data\\clean1")
    spark.stop()

  }
}
