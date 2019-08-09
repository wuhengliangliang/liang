package com

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期时间解析工具类
  * 10/Nov/2016:00:01:02 +0800
 */
object DateUtils {


    //输入文件日期格式
    //10/Nov/2016:00:01:02 +0800
    val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
    //目标文件日期格式
    val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    /**
      * 获取时间：yyyy-MM-dd HH:mm:ss
      */
    def parse(time:String)={
      TARGET_FORMAT.format(new Date(getTime(time)))
    }

    /**
      * 获取输入日志的时间
      * 日期格式
      *[10/Nov/2016:00:01:02 +0800]
      *
      */
    def getTime(time:String) ={
            //注意需要转换为Long类型的数据
      try{
        YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[")+1,time.lastIndexOf("]"))).getTime
      }catch {
        case e:Exception =>{//如果输入的时间异常：那么就是直接转换为0
          0l
        }
      }
    }

    def main(args: Array[String]){
      println(parse("[10/Nov/2016:00:01:02 +0800]"))
    }

}
