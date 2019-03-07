package com.gmall.offline.handle

import com.gmall.common.datamodules.UserVisitAction
import com.gmall.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD

/**
  * @aythor HeartisTiger
  *         2019-03-07 16:39
  */
object Top10SessionHandle {
  def handle(categoryCountInfo:List[CategoryCountInfo],uservisitRDD:RDD[UserVisitAction]) ={


  }
}
