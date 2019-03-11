package com.gmall.realtime.handle

import com.gmall.common.utils.{PropertiesUtil, RedisUtil}
import com.gmall.realtime.app.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
  * @aythor HeartisTiger
  *         2019-03-11 11:31
  */
object BluckListHandler {
  def handle(adsLogDstream: DStream[AdsLog]) = {

      adsLogDstream.foreachRDD(adslogRDD => {
        val adslog = adslogRDD.map(adslog => (adslog.getTS2Date() + "_" + adslog.user_id + "_" + adslog.ads_id, 1L))
        val adslogcount = adslog.reduceByKey(_ + _)
        adslogcount.foreachPartition{
          keyItr=>
            val jedis = RedisUtil.getJedisClient
            keyItr.foreach{
              case (key,count)=>
                val day = key.split("_")(0)
                val uid = key.split("_")(1)
                val aid = key.split("_")(2)
                jedis.hincrBy(day,uid+"_"+aid,count)

                val curCount = jedis.hget(day,uid+"_"+aid)
                if(curCount.toInt >=100){
                  jedis.sadd("blacklist",uid)
                }
            }
            jedis.close()
        }

      }
      )
  }
  def check(sc:SparkContext, adsLogDstream: DStream[AdsLog])={


    val filterDStream = adsLogDstream.transform { rdd =>
      val blacklist = RedisUtil.getJedisClient.smembers("blacklist")

      val blacklistBC = sc.broadcast(blacklist)
      rdd.filter {
        adslog =>
          !blacklistBC.value.contains(adslog.user_id)
      }
    }
    filterDStream

  }






}




