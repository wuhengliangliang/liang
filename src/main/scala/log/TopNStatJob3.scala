package log
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
object TopNStatJob3 {
  /**
    * TopN统计spark作业
    */
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TopNStatJob3")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .master("local[2]").getOrCreate()
    val accessDF = spark.read.format("parquet").load("D:\\BigDataTest\\data\\clean")
    accessDF.printSchema()
    accessDF.show()

    /**
      * 按照地市统计topN的课程
      *
      */
    val day = "20170511"
    StatDao3.deleteData(day)
    videoTrafficsTopNStat(spark,accessDF,day)


    spark.stop()
  }

  /**
    * 按照流量进行统计TopN课程
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF:DataFrame,day:String ): Unit = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day"===day && $"cmsType"==="video")
      .groupBy("day","cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)//.show(false)

    //cityAccessTopNDF.show(false)

    //Window函数在Spark SQL的使用


    /**
      * 将统计结果写入到MySQL中
      */
    try {
     cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficsStat(day,cmsId,traffics))
        })

        StatDao3.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}