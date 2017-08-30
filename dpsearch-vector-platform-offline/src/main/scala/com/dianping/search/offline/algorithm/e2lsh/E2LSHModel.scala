package com.dianping.search.offline.algorithm.e2lsh

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by zhen.huaz on 2017/8/11.
  */

/**
  * minhash模型的构造方法
  * @param n 表示生成构建hash值种子的上限值,>=item最大值
  * @param distance 表示生成拆分的距离度量
  * @param numRows 表示hash表的个数
  */
class E2LSHModel(n : Int, distance : Int ,numRows : Int,k : Int,ts : Int) extends Serializable {

  val tablesize = ts
  private val C  = (Math.pow(2, 32) - 5).toInt
  /** generate rows hash functions */
  private val _hashFunctions = ListBuffer[E2Hasher]()
  //生成hash表列表
  for (i <- 0 until numRows)
    _hashFunctions += E2Hasher.create(n,distance)
  //为hash表添加id
  final val hashFunctions : List[(E2Hasher, Int)] = _hashFunctions.toList.zipWithIndex

  val fpRand = ListBuffer[Int]()

  val random = new Random
  for(i <- 0 until k) {
    fpRand += (Math.random( )*20 -10).toInt
  }

  /** 签名矩阵 */
  var signatureMatrix : RDD[List[Int]] = null

  /** the "bands" ((hash of List, band#), row#) */
//  var bands : RDD[((Int, Int), Iterable[String])] = null
  var bands : RDD[((Int,Int), Iterable[String])] = null

  /** (vector id, cluster id) */
  var vector_cluster : RDD[(String, Long)] = null

  /** (cluster id, vector id) */
  var cluster_vector : RDD[(Long, String)] = null

  /** (cluster id, List(Vector) */
  var clusters : RDD[(Long, Iterable[DenseVector])] = null

  /** jaccard cluster scores */
  var scores : RDD[(Long, Double)] = null

  /** **/
  var vector_hashlist : RDD[(String, List[(Int,Int)])] = null

  /** filter out scores below threshold. this is an optional step.*/
  def filter(score : Double) : E2LSHModel = {

    val scores_filtered = scores.filter(x => x._2 > score)
    val clusters_filtered = scores_filtered.join(clusters).map(x => (x._1, x._2._2))
    val cluster_vector_filtered = scores_filtered.join(cluster_vector).map(x => (x._1, x._2._2))
    scores = scores_filtered
    clusters = clusters_filtered
    cluster_vector = cluster_vector_filtered
    vector_cluster = cluster_vector.map(x => x.swap)
    this
  }

  //def compare(SparseVector v) : RDD
  def H2(hashVals : Array[Int]): Int ={
    var re : Long = 0
    for(i <- 0 until hashVals.size) {
      re += hashVals(i) * fpRand(i)
    }
    re = re % C
    if(re < 0 )
      re = re + C
    re.toInt
  }

}