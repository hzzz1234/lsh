package com.dianping.search.offline.algorithm.cosinelsh

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by zhen.huaz on 2017/8/11.
  */

/**
  * minhash模型的构造方法
  * @param dimension 表示生成构建随机向量的维度值
  */
class CosineLSHModel(dimension : Int, numRows : Int) extends Serializable {

  /** generate random vector */
  private val _hashVectors = ListBuffer[CosineHashVector]()
  //生成hash表列表
  for (i <- 0 until numRows)
    _hashVectors += CosineHashVector.create(dimension)
  //为hash表添加id
  final val hashVectors : List[(CosineHashVector, Int)] = _hashVectors.toList.zipWithIndex

  /** 签名矩阵 */
  var signatureMatrix : RDD[List[Int]] = null

  /** the "bands" ((hash of List, band#), row#) */
//  var bands : RDD[((Int, Int), Iterable[String])] = null
  var bands : RDD[((String,Int), Iterable[String])] = null

  /** (vector id, cluster id) */
  var vector_cluster : RDD[(String, Long)] = null

  /** (cluster id, vector id) */
  var cluster_vector : RDD[(Long, String)] = null

  /** (cluster id, List(Vector) */
  var clusters : RDD[(Long, Iterable[DenseVector])] = null

  /** jaccard cluster scores */
  var scores : RDD[(Long, Double)] = null

  /** **/
  var vector_hashlist : RDD[(String,List[String])] = null

  /** filter out scores below threshold. this is an optional step.*/
  def filter(score : Double) : CosineLSHModel = {

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


}