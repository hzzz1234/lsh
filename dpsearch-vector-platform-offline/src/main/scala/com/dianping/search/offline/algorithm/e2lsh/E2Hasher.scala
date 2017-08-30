package com.dianping.search.offline.algorithm.e2lsh

import com.dianping.search.offline.algorithm.common.GaussianRandomNumberGenerator
import com.github.fommil.netlib.F2jBLAS
import org.apache.spark.mllib.linalg.SparseVector

import scala.util.Random

/**
  * Created by zhen.huaz on 2017/8/11.
  * simple hashing function. defined by ints a, b, p, m
  * where a and b are seeds with a > 0.
  * p is a prime number, >= u (largest item in the universe)
  * m is the number of hash bins
  */

class E2Hasher(a : Array[Double], b : Double, distance : Int) extends Serializable {

  override def toString(): String = {
    val sb = new StringBuilder
    sb.append("([")
    for( ele <- a ) {
      sb.append("%06.5f".format(ele)+" ")
    }
    sb.append("]")
    sb.append(", " + b +", " + distance + ")")
    sb.toString()
  }

  def e2hash(v : Array[Double]) : Int = {
    return Math.round((E2Hasher.f2jBLAS.ddot(v.size,a,1,v,1) + b) / distance).toInt
  }

}

object E2Hasher {
  /** create a new instance providing p and m. a and b random numbers mod p */
  def create(n : Int , w : Int) = new E2Hasher(a(n), b(w), w)

  val f2jBLAS = new F2jBLAS()

  /** create a seed "a" */
  def a(n : Int) : Array[Double] = {
    return GaussianRandomNumberGenerator.create(n)
  }

  /** create a seed "b" */
  def b(w : Int) : Double = {
    return w * new Random().nextDouble()
  }

}