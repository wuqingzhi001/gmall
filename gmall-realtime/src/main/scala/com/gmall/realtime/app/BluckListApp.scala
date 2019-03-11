package com.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.gmall.common.utils.MyKafkaUtil
import com.gmall.realtime.handle.BluckListHandler
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @aythor HeartisTiger
  *         2019-03-11 11:13
  */
object BluckListApp {





  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("blucklistapp")
    val sc = new SparkContext(sparkconf)
    val spark = new StreamingContext(sc,Seconds(5))

    val adslogStream = MyKafkaUtil.getKafkaStream("adslog",spark)
    val adsLogDstream = adslogStream.map(_.value()).map { adslog =>
      val fields = adslog.split(" ")
      AdsLog(fields(0).toLong,fields(1),fields(2),fields(3),fields(4))
    }
    val value = BluckListHandler.check(sc,adsLogDstream)

    BluckListHandler.handle(value)


    spark.start()
    spark.awaitTermination()

  }
}
case class AdsLog(ts:Long,area:String,city:String,user_id:String,ads_id:String){
  var simp = new SimpleDateFormat("yyyy-MM-dd")
  def getTS2Date()={
    simp.format(new Date(ts))
  }
}
