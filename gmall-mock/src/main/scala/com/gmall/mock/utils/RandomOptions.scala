package com.gmall.mock.utils

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * @aythor HeartisTiger
  *         2019-03-06 20:03
  */
object RandomOptions {

  def apply[T](opts:RanOpt[T]*): RandomOptions[T] ={
    val randomOptions=  new RandomOptions[T]()
    for (opt <- opts ) {
      randomOptions.totalWeight+=opt.weight
      for ( i <- 1 to opt.weight ) {
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions
  }

}


case class RanOpt[T](value:T,weight:Int){
}
class RandomOptions[T](opts:RanOpt[T]*) {
  var totalWeight=0
  var optsBuffer  =new ListBuffer[T]

  def getRandomOpt(): T ={
    val randomNum= new Random().nextInt(totalWeight)
    optsBuffer(randomNum)
  }
}
