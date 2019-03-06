package com.gmall.mock.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * @aythor HeartisTiger
  *         2019-03-06 20:01
  */
object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复
    //RandomNum.multi(1,cargoryNum,RandomNum(1,5),",",false)

    if(canRepeat){
      val list =   ListBuffer[Int]()
      while(list.size<amount){
        list += apply(fromNum,toNum)
      }
      list.mkString(delimiter)
    }else {
      val set =   new mutable.HashSet[Int]()
      while(set.size<amount){
        set += apply(fromNum,toNum)
      }
      set.mkString(delimiter)
    }

  }


}
