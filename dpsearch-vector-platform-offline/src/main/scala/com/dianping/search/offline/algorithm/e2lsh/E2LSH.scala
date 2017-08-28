package com.dianping.search.offline.algorithm.e2lsh

import java.lang.Math._

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.rdd.RDD

/**
  * Created by zhen.huaz on 2017/8/11.
  * support format : (vectorid,vector)
  */
class E2LSH (data : RDD[(String,DenseVector)], w : Int, numRows : Int, numBands : Int, ts : Int ,minClusterSize : Int) extends Serializable {

  /** run LSH using the constructor parameters */
  def run() : E2LSHModel = {
    val n = data.first()._2.size

    //创建一个minhash的模型
    val model = new E2LSHModel(n,w,numRows,numRows/numBands,ts)


    //compute signatures from matrix
    // - hash each vector <numRows> times
    // - position hashes into bands. we'll later group these signature bins and has them as well
    //this gives us ((vector idx, band#), minhash)
    val signatures = data.flatMap(v => model.hashFunctions.flatMap(h => List(((v._1, h._2 % numBands),h._1.e2hash(v._2.toArray))))).cache()
    val mid = signatures.groupByKey().map(x => ((model.H2(x._2.toArray) % model.tablesize,model.H2(x._2.toArray)),x._1._1)).cache()


    model.vector_hashlist = data.join(mid.map(x => x.swap).groupByKey().map(x => (x._1,x._2.toList))).map(row => (row._1,(row._2._1,row._2._2)))

      //reorganize data for shuffle
    //this gives us ((band#, hash of minhash list), vector id)
    //groupByKey gives us items that hash together in the same band
    model.bands = mid.groupByKey().cache()

    //we only want groups of size >= <minClusterSize>
    //(vector id, cluster id)
    model.vector_cluster = model.bands.filter(x => x._2.size >= minClusterSize) //获取大于2的
      .map(x => x._2.toList.sorted) //大于2的所有id排序
      .distinct()
      .zipWithIndex() //为每个向量组独立一个id
      .map(x => x._1.map(y => (y, x._2)))
      .flatMap(x => x.grouped(1))
      .map(x => x(0)).cache()

    //(cluster id, vector id)
    model.cluster_vector = model.vector_cluster.map(x => x.swap).cache()

    //(cluster id, List(vector))
    model.clusters = data.join(model.vector_cluster).map(x => (x._2._2, x._2._1)).groupByKey().cache()

    //compute the jaccard similarity of each cluster
    model.scores = model.clusters.map(row => (row._1, e2distance(row._2.toList))).cache()

    model
  }


  /** compute a single vector against an existing model */
  def compute(data : DenseVector, model : E2LSHModel, minScore : Double) : RDD[(Long, Iterable[DenseVector])] = {
    model.clusters.map(x => (x._1, x._2++List(data))).filter(x => e2distance(x._2.toList) >= minScore)
  }

  def e2distance(l : List[DenseVector]) : Double = {
    if(l.size < 1){
      return 0
    }
    var base = new Array[Double](l(0).size)
    for(ele <- l){
      sum(base,ele.toArray)
    }
    for( i <- 0 until base.size){
      base(i) = base(i)/l.size
    }
    var re = 0.0
    for(ele <- l){
      re += e2distance(base,ele.toArray)
    }
    return re/l.size
  }

  def sum(a : Array[Double], b : Array[Double]) : Unit = {
    for( i <- 0 until a.size){
      a(i) = a(i) + b(i)
    }
  }

  def e2distance(a : DenseVector, b : DenseVector) : Double = {
    if(a.size != b.size)
    return 0
    val al = a.toArray
    val bl = b.toArray

    var norm = 0.0
    var a1 : Double = 0.0
    var b1 : Double = 0.0
    for( i <- 0 until al.size) {
      if (al(i) != 0)
        a1 = al(i)
      else
        a1 = 0

      if (bl(i) != 0)
        b1 = bl(i)
      else
        b1 = 0
      if (a1 != 0 && b1 != 0) {
        norm += pow((b1 - a1), 2)
      }
    }

    return norm
  }

  def e2distance(a : Array[Double], b : Array[Double]) : Double = {
    if(a.size != b.size)
      return 0

    var norm = 0.0
    var a1 : Double = 0.0
    var b1 : Double = 0.0
    for( i <- 0 until a.size) {
      if (a(i) != 0)
        a1 = a(i)
      else
        a1 = 0

      if (b(i) != 0)
        b1 = b(i)
      else
        b1 = 0
      if (a1 != 0 && b1 != 0) {
        norm += pow((b1 - a1), 2)
      }
    }

    return norm
  }

}