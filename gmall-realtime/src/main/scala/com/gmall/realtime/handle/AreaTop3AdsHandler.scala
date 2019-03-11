package com.gmall.realtime.handle

import java.util

import com.gmall.common.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._


/**
  * @aythor HeartisTiger
  *         2019-03-11 21:10
  */
object AreaTop3AdsHandler {
  def handle(adsClickCount: DStream[(String, Long)]) = {
    val AdsCountPerArea = adsClickCount.map { case (key, count) =>
      val fields = key.split(":")
      (fields(0) + ":" + fields(1) + ":" + fields(3), count)
    }.reduceByKey(_ + _)
    val areaAdsCount = AdsCountPerArea.map { case (key, count) =>
      val fields = key.split(":")

      (fields(0) + ":" + fields(1), (fields(2), count))
    }.groupByKey()
    val adsTop3List = areaAdsCount.map { case (key, adscountItr) =>
      val list = adscountItr.toList.sortWith(_._2 > _._2).take(3)
      val jsonstring = JsonMethods.compact(JsonMethods.render(list))
      (key,jsonstring)
    }
    //(2019-03-11:华北,List((1,390), (6,388), (5,385)))
    adsTop3List.foreachRDD(rdd=>

      rdd.foreachPartition(rddItr=> {
        val jedis = RedisUtil.getJedisClient
        rddItr.foreach{case (datearea,json)=>
          jedis.hset("top3_ads_per_day:"+datearea.split(":")(0),datearea.split(":")(1),json)
        }
        jedis.close()
      }
      )
    )



  }

}
