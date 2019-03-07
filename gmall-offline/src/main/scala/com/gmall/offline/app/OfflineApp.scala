package com.gmall.offline.app

import com.alibaba.fastjson.JSON
import com.gmall.common.datamodules.UserVisitAction
import com.gmall.common.utils.{JdbcUtil, PropertiesUtil}
import com.gmall.offline.bean.Top10SessioonByCid
import com.gmall.offline.handle.{Top10CategoryHandle, Top10SessionHandle}
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
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //将数据从hive中取出来并且构建为RDD
    val userVisitActionRDD = readUserVisitActionToRDD(sparkSession)
    //print(userVisitActionRDD.collect().mkString(","))

    val top10CategoryList = Top10CategoryHandle.top10CategoryHandle(sparkSession, userVisitActionRDD)
    //Top10SessionHandle.handle(top10CategoryList,userVisitActionRDD)
    //获取top的品类
    val catetop10 = top10CategoryList.map(_.category_id)

    //将top10的品类弄成广播变量
    val top10CateBK = sparkSession.sparkContext.broadcast(catetop10)

    // 将top10品类下的数据筛选出来
    val filterRDD = userVisitActionRDD.filter {
      case cate =>
        if (top10CateBK.value.contains(cate.click_category_id.toString)) {
          true
        } else {
          false
        }
    }
    val catelist = filterRDD.map {
      case userVisit =>
        (userVisit.click_category_id + "_" + userVisit.session_id, 1L)
    }.reduceByKey(_ + _).map {
      case (key, count) =>
        val cid = key.split("_")(0)
        val s_id = key.split("_")(1)
        (cid, Top10SessioonByCid("02",cid,s_id,count))
    }.groupByKey()
    val top10ListRDD = catelist.map {
      case (cid, comItr) =>
        val top10 = comItr.toList.sortWith((x, y) =>
          if (x.clickCount > y.clickCount) {
            true
          } else {
            false
          }
        ).take(10)
        top10
    }
    val top10RDD = top10ListRDD.flatMap(cate =>
      cate
    )
    val rdd = top10RDD.map(top =>
      Array(top.taskId, top.category_id, top.sessionId, top.clickCount)
    ).collect()

      JdbcUtil.executeBatchUpdate("insert into sessiontop10bycid values(?,?,?,?) ",rdd)


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
   //userVisitActionDateset.foreach(println)
    userVisitActionDateset
  }
}
