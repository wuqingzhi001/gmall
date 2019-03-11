package com.gmall.realtime.app

import com.gmall.common.utils.{MyKafkaUtil, RedisUtil}
import com.gmall.realtime.handle.AreaCityAdsCountHandle
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.thrift.Option.Some

/**
  * @aythor HeartisTiger
  *         2019-03-11 19:19
  */
object AreaCityAdsCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("areacityadscount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoint")
    val ssc = new StreamingContext(sc, Seconds(5))

    val adslogStream = MyKafkaUtil.getKafkaStream("adslog", ssc)
    AreaCityAdsCountHandle.handle(adslogStream)


    ssc.start()
    ssc.awaitTermination()
  }
}
