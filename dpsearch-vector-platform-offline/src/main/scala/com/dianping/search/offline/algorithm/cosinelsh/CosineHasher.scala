package com.dianping.search.offline.algorithm.cosinelsh

import com.dianping.search.offline.algorithm.common.{GaussianRandomNumberGenerator, RandomNumberGenerator}
import com.github.fommil.netlib.F2jBLAS
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}

import scala.util.Random

/**
  * Created by zhen.huaz on 2017/8/11.
  * simple hashing function. defined by ints a, b, p, m
  * where n is size of vector
  */

class CosineHashVector(n : Int,vector: Array[Double]) extends Serializable {

  def this(n : Int) {
    this(n,GaussianRandomNumberGenerator.create(n))
  }

  def this(n : Int,randomNumberGenerator: RandomNumberGenerator) {
    this(n,randomNumberGenerator.getVector)
  }

  override def toString(): String = {
    val sb = new StringBuilder
    sb.append("[")
    for( ele <- vector ) {
      sb.append("%.5f".format(ele)+" ")
    }
    sb.append("]")
    sb.toString()
  }


  def cosinehash(v : DenseVector) : Int = {
    val re = CosineHashVector.f2jBLAS.ddot(vector.size,vector,1,v.toArray,1)
    if(re > 0)
      return 1;
    else
      return 0;
  }

  def cosinehash(v : Array[Double]) : Int = {
    val re = CosineHashVector.f2jBLAS.ddot(vector.size,vector,1,v,1)
    println(re)

    if(re > 0)
      return 1;
    else
      return 0;
  }

}

object CosineHashVector {
  /** create a new instance providing p and m. a and b random numbers mod p */
  def create(n : Int) = new CosineHashVector(n)

  val f2jBLAS = new F2jBLAS()

  def main(args: Array[String]) {
    var cosineHashVector = CosineHashVector.create(100);
    var v = GaussianRandomNumberGenerator.create(100)
    var r = cosineHashVector.cosinehash(v)
    println(cosineHashVector)
    val sb = new StringBuilder
    sb.append("[")
    for( ele <- v ) {
      sb.append(ele+" ")
    }
    sb.append("]")
    println(sb.toString)
    println(r)
  }
}