package log

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
/**
  * TopN统计spark作业
  */
object TopNStatJob {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled",false)
      .master("local[2]").getOrCreate()
    val accessDF = spark.read.format("parquet").load("D:\\BigDataTest\\data\\clean")
    accessDF.printSchema()
    accessDF.show()
    StatDao3.deleteData("20170511")
    //最受欢迎的topN的课程个数
    videoAccessTopNstat(spark,accessDF)

    spark.stop()
  }




  /**
    * 最受欢迎的topN的课程个数
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNstat(spark: SparkSession,accessDF: DataFrame): Unit ={
    /**
      * 使用DataFrame的方式进行统计
      */
    //    //隐式转换
    import spark.implicits._
    //过滤出当天的视频的个数
    val videoAccessTopNDF = accessDF.filter($"day"==="20170511" && $"cmsType"==="video")
      .groupBy("day","cmsId")
      .agg(count("cmsId")as("times"))
      .orderBy($"times".desc)
    videoAccessTopNDF.show(false)

    //方法二采用sparkSql的方式
//    accessDF.createOrReplaceTempView("access_log")
//   val videoAccessTopNDF = spark.sql("select day,cmsId,count(1) as times from access_log "
//      + "where day='20170511' and cmsType= 'article' "
//      + "group by day,cmsId order by times desc")
//   videoAccessTopNDF.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          /**
            * 不建议大家在此处进行数据库的数据插入
            */
          list.append(DayVideoAccessStat(day, cmsId, times))
        })
          StatDao.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
