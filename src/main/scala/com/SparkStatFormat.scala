package com

import org.apache.spark.sql.SparkSession

/**
 * 第一步清洗：抽取出我们想要的指定列的数据
 */
object SparkStatFormat {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    //读取数据
    val access = spark.sparkContext.textFile("D:\\BigDataTest\\data\\access.20161111.log")
    //打印出前面100条数据
    //    access.take(100).foreach(println)
    //按照空格进行数据分割
    access.map(line =>{
      val splits = line.split(" ")
      val ip = splits(0)//第一个为ip地址
      //根据断点调试可以看到时间在第几个split的数组当中
      /**
      * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间
        * (183.162.52.7,[10/Nov/2016:00:01:02 +0800]) ==》yyyy-MM-dd HH:mm:ss
        * 这个时间需要时间解析
      **/
      val time = splits(3)+" "+splits(4)
      //拿取url,经过反复调试可以观察到split(11)为url
      //把不必要的引号替换为空的
      val url = splits(11).replaceAll("\"","")
      //流量
      val traffic = splits(9)

      //利用元组的形式存放
      //用写的时间转换的工具类来转换时间
//      (ip,DateUtils.parse(time),url,traffic)
      DateUtils.parse(time)+"\t"+url+"\t"+traffic+"\t"+ip
    }).coalesce(1,true).saveAsTextFile("D:\\BigDataTest\\data\\output\\demo")


    spark.stop()
  }

}
