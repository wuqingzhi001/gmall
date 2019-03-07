package com.gmall.offline.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @aythor HeartisTiger
  *         2019-03-07 19:33
  */
object AreaCountApp {
  def main(args: Array[String]): Unit = {
    var sparkconf = new SparkConf().setAppName("demo03").setMaster("local[*]")
    var sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()
    sparkSession.sql("use gmall")
  sparkSession.sql("select c.area,u.click_product_id,c.city_name from user_visit_action u join city_info c on c.city_id=u.city_id  where u.click_product_id >0").createOrReplaceTempView("t1")

    sparkSession.sql("select area,click_product_id,count(*) , city_ratio(city_name) clickcount " +

      "from t1 group by area,click_product_id ").createOrReplaceTempView("t2")
    sparkSession.sql("select * ,rank() over(partition by area order by clickcount desc) rk from t2").createOrReplaceTempView("t3")
    sparkSession.sql("select area,click_product_id,clickcount from t3 where rk <=3 ").show(100)
}
}
