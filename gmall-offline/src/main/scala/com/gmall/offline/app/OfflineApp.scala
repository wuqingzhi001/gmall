package com.gmall.offline.app

import com.alibaba.fastjson.JSON
import com.gmall.common.datamodules.UserVisitAction
import com.gmall.common.utils.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @aythor HeartisTiger
  *         2019-03-06 22:48
  */
object OfflineApp {
  def main(args: Array[String]): Unit = {
    //1.从hive中将数据取出来转换为RDD[action]
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")

    //构建hive sparkSession
    var sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //将数据从hive中取出来并且构建为RDD
    readUserVisitActionToRDD(sparkSession)
  }

  //
  def readUserVisitActionToRDD(sparkSession: SparkSession)={

      //根据条件拼接sql
    val conditions = PropertiesUtil.load("conditions.properties")

    val conditionsObj = JSON.parseObject(conditions.getProperty("condition.params.json"))

    var sql = new StringBuilder("select v.* from user_visit_action v join user_info u on  u.user_id = v.user_id where  ");

    //从json中取出来参数
    var startDate = conditionsObj.getString("startDate")

    var endDate = conditionsObj.getString("endDate")

    var startAge = conditionsObj.getString("startAge")

    var endAge = conditionsObj.getString("endAge")

    sql.append("date >= "+"'"+startDate+"' and ")

    sql.append("date <= "+"'"+endDate+"' and ")

    sql.append("age >= "+startAge+" and ")
    sql.append("age <= "+endAge)

    println(sql.toString())
    import sparkSession.implicits._
    sparkSession.sql("use gmall ")
    val userVisitActionDateset = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd
    userVisitActionDateset.foreach(println)




  }
}
