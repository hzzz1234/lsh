package com.dianping.search.offline.algorithm.minhash

import org.apache.spark.mllib.linalg.SparseVector

import scala.util.Random
/**
  * Created by zhen.huaz on 2017/8/11.
  * simple hashing function. defined by ints a, b, p, m
  * where a and b are seeds with a > 0.
  * p is a prime number, >= u (largest item in the universe)
  * m is the number of hash bins
  */

class MinHasher(a : Int, b : Int, p : Int, m : Int) extends Serializable {

  override def toString(): String = "(" + a + ", " + b + ")";

  def hash(x : Int) : Int = {
    ( ((a.longValue*x) + b) % p ).intValue % m
  }

  def minhash(v : SparseVector) : Int = {
    v.indices.map(i => hash(i)).min
  }

}

object MinHasher {
  /** create a new instance providing p and m. a and b random numbers mod p */
  def create(p : Int, m : Int) = new MinHasher(a(p), b(p), p, m)



  /** create a seed "a" */
  def a(p : Int) : Int = {
    val r = new Random().nextInt(p)
    if(r == 0)
      a(p)
    else
      r
  }

  /** create a seed "b" */
  def b(p : Int) : Int = {
    new Random().nextInt(p)
  }

}