package com.gmall.realtime.handle

import com.gmall.common.utils.RedisUtil
import com.gmall.realtime.app.AdsLog
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream


/**
  * @aythor HeartisTiger
  *         2019-03-11 19:46
  */
object AreaCityAdsCountHandle {
  def handle(adslogStream: InputDStream[ConsumerRecord[String, String]]) = {
    val adsDStream = adslogStream.map(
      rdd => {
        val fiels = rdd.value().split(" ")

        AdsLog(fiels(0).toLong, fiels(1), fiels(2), fiels(3), fiels(4))
      }
    ).map(
      adslog => (adslog.getTS2Date() + ":" + adslog.area + ":" + adslog.city + ":" + adslog.ads_id, 1L)
    )

    val adsClickCount = adsDStream.updateStateByKey { (countSeq: Seq[Long], total: Option[Long]) =>
      val sum = countSeq.sum

      val curTotal = total.getOrElse(0L) + sum
      Some(curTotal)
    }

    AreaTop3AdsHandler.handle(adsClickCount)
    adsClickCount.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          rddItr=>
            val jedis = RedisUtil.getJedisClient
            rddItr.foreach{case (key,count)=>
              jedis.hset("date:area:city:ads",key,count.toString)
            }
            jedis.close()
        }
    }

  }

}
