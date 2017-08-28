package com.dianping.search.offline.algorithm.common

import org.apache.commons.math3.random.{JDKRandomGenerator, RandomGenerator}

/**
  * Created by zhen.huaz on 2017/8/15.
  */
class GaussianRandomNumberGenerator extends RandomNumberGenerator{
  var N: Int = 0
  var rg: RandomGenerator = new JDKRandomGenerator

  def this(N: Int, rg: RandomGenerator) {
    this()
    this.N = N
    this.rg = rg
  }

  def this(N: Int) {
    this()
    this.N = N
  }

  def nextNormalizedDouble: Double = {
    return rg.nextGaussian
  }

  @Override
  def getVector: Array[Double] = {
    val vector: Array[Double] = new Array[Double](N)
    var i: Int = 0
    while (i < vector.length) {
      {
        vector(i) = nextNormalizedDouble
      }
      ({
        i += 1; i
      })
    }
    return vector
  }
}

object GaussianRandomNumberGenerator{
  def create (n : Int) : Array[Double] = new GaussianRandomNumberGenerator(n).getVector
}