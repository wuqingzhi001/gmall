package com.gmall.offline.handle

import com.gmall.common.datamodules.UserVisitAction
import com.gmall.common.utils.JdbcUtil
import com.gmall.offline.acc.CategoryAcc
import com.gmall.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @aythor HeartisTiger
  *         2019-03-07 13:09
  */
object Top10CategoryHandle {
  def top10CategoryHandle(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction]) = {
    //制作累加器
    val acc = new CategoryAcc()
    sparkSession.sparkContext.register(acc)
    println(userVisitActionRDD)
    userVisitActionRDD.foreach { userVisitAction =>
      if (userVisitAction.click_category_id != -1L) {
        acc.add(userVisitAction.click_category_id + "_click")
      }
      else if (userVisitAction.order_category_ids != null && userVisitAction.order_category_ids.length > 0) {
        userVisitAction.order_category_ids.split(",").foreach(cid =>
          acc.add(cid + "_order")
        )
      }
      else if (userVisitAction.pay_category_ids != null && userVisitAction.pay_category_ids.length > 0) {
        userVisitAction.pay_category_ids.split(",").foreach(cid =>
          acc.add(cid + "_pay")
        )
      }
    }
    val map = acc.value
    //println(map.mkString(","))

    val catecountList = map.groupBy({ case (key, count) => key.split("_")(0) }).map { case (cid, map) => {
      CategoryCountInfo("01", cid, map.getOrElse(cid + "_click", 0L), map.getOrElse(cid + "_order", 0L), map.getOrElse(cid + "_pay", 0L))
    }
    }.toList.sortWith(
      (cate1, cate2) => {
        if (cate1.click_count > cate2.click_count) {
          true
        } else if (cate1.click_count == cate2.click_count) {
          if (cate1.order_count > cate2.order_count) {
            true
          } else if (cate1.order_count == cate2.order_count) {
            if (cate1.pay_count > cate2.pay_count) {
              true
            } else {
              false
            }

          } else {
            false
          }
        } else {
          false
        }
      }

    )
    val cateTop10List = catecountList.take(10)
    val array = cateTop10List.map(cate =>
      Array(cate.taskId, cate.category_id, cate.click_count, cate.order_count, cate.pay_count)
    )
    //JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", array)
    cateTop10List
  }

}
