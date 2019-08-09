package log
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
object TopNStatJob2 {
  /**
    * TopN统计spark作业
    */
    def main(args: Array[String]) {
      val spark = SparkSession.builder().appName("TopNStatJob2")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
        .master("local[2]").getOrCreate()
      val accessDF = spark.read.format("parquet").load("D:\\BigDataTest\\data\\clean")
      accessDF.printSchema()
      accessDF.show()

      /**
        * 按照地市统计topN的课程
        *
        */
      cityAccessTopNStat(spark,accessDF)


      spark.stop()
    }
    /**
      * 按照地市进行统计
      * accessDF
      */
  /**
    * 按照地市进行统计TopN课程
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF:DataFrame): Unit = {
    import spark.implicits._
//    //过滤出当天的视频的个数
    val cityAccessTopNDF = accessDF.filter($"day"==="20170511" && $"cmsType"==="video")
      .groupBy("day","city","cmsId")
      .agg(count("cmsId").as("times"))

    //cityAccessTopNDF.show(false)

    //Window函数在Spark SQL的使用

    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <=3")//.show(false)  //Top3
    /**
      * 将统计结果写入到MySQL中
      */
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day,cmsId,city,times,timesRank))
        })

        StatDao2.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}