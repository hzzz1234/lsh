package com.dianping.search.offline.utils

import scala.util.control.Breaks

/**
  * Created by zhen.huaz on 2017/8/30.
  */
object PrimeUtils {
  def primes(n : Int) : Int = {
    val loop1 = new Breaks
    val loop = new Breaks
    var isPrime : Boolean = true
    var result = 0
    loop1.breakable(
      for( i <- Range(n,n*2)){
        isPrime = true
        result = i
        loop.breakable(
          for(j <- Range(2,i/2)) {
            if(i%j == 0) {
              isPrime = false
              loop.break()
            }
          }
        )
        if(isPrime == true){
          loop1.break()
        }
      }
    )
    return result
  }

  def main(args: Array[String]) {
    println(PrimeUtils.primes(65535))
  }
}
