package com.dianping.search.offline.algorithm.cosinelsh

import java.lang.Math._

import com.dianping.search.offline.utils.MD5Utils
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * Created by zhen.huaz on 2017/8/11.
  * support format : (vectorid,vector)
  */
/**
  *
  * @param origin_data  一般使用密集矩阵计算
  * @param dimension 向量维度
  * @param numRows  稀疏签名矩阵的行数
  * @param numBands band数(用于拆分稀疏签名矩阵为多个局部相似度量)
  * @param minClusterSize 最小聚合数(一般为2)
  */
class CosineLSHForIncr (origin_data : RDD[(String,Vector)], dimension : Int, numRows : Int, numBands : Int, minClusterSize : Int,hash : List[(CosineHashVector, Int)]) extends Serializable {

  def rank(toList: List[(String, Int)]) = {
    val arr = new Array[String](toList.length);
    for( ele <- toList){
      arr(ele._2) = ele._1
    }
    arr.toList
  }

  /** run CosineLSH using the constructor parameters */
  def run() : CosineLSHModelForIncr = {

    //创建一个minhash的模型
    val model = new CosineLSHModelForIncr(dimension,numRows,hash )

    val data = origin_data.map(line => (line._1,line._2.toDense)).cache
    // 计算签名矩阵
    // - 对向量hash numrows次
    // - 将hash后的值定位到对应的band中,后面会根据band号进行分组,构建局部签名的桶
    // output ((vector idx, band#), minhash)
    val signatures = data.flatMap(v => model.hashVectors.flatMap(h => List(((v._1, h._2 % numBands),h._1.cosinehash(v._2.toDense)))))

    // 对同band下的数据进行签名
    // output ((band+minlist)->(bandhashvalue,hashid),key)
    val mid = signatures.groupByKey().map(x => ((transforMD5(x._1._2,x._2),x._1._2), x._1._1)).cache()
    print(1)
    // output (key,hashlist)
    model.vector_hashlist = data.join(mid.map(x => x.swap).groupByKey().map(x => (x._1,rank(x._2.toList)))).map(row => (row._1,row._2._2)).cache()


    // 组织所有在同一个band且minhashlist的值一样的数据合并到一起
    // output ((bandhashvalue,hashid), vectorid list)
    model.bands = mid.groupByKey().cache()

    //找到所有在聚合点数大于2点聚合id
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

    //compute the cosine similarity of each cluster
    model.scores = model.clusters.map(row => (row._1, cosine(row._2.toList))).cache()

    model
  }

  def cosine(l: List[DenseVector]) : Double = {
    if(l.size < 1){
      return 0
    }
    var base = new Array[Double](l(0).size)
    for(ele <- l){
      sum(base,one_vector(ele))
    }

    var re = 0.0
    for(ele <- l){
      re += cosine(base,ele.toArray)
    }
    return re/l.size
  }

  def sum(a : Array[Double], b : Array[Double]) : Unit = {
    for( i <- 0 until a.size){
      a(i) = a(i) + b(i)
    }
  }
  def transforMD5(band : Int, iterable: Iterable[Int]): String ={
    val sb = new StringBuilder
    sb.append("[")
    for (it <- iterable) {
      sb.append(it+" ")
    }
    sb.append("]"+band)
    MD5Utils.md5Hash(sb.toString)
  }

  def transfor(band : Int, iterable: Iterable[Int]): String ={
    val sb = new StringBuilder
    sb.append("[")
    for (it <- iterable) {
      sb.append(it+" ")
    }
    sb.append("]"+band)
    sb.toString()
  }

  /** compute a single vector against an existing model */
  def compute(data : DenseVector, model : CosineLSHModel, minScore : Double) : RDD[(Long, Iterable[DenseVector])] = {
    model.clusters.map(x => (x._1, x._2++List(data))).filter(x => cosine(x._2.toList) >= minScore)
  }


  def cosine(a : Array[Double], b : Array[Double]) : Double = {
    if(a.size != b.size)
      return 0

    val dot_product = CosineHashVector.f2jBLAS.ddot(a.size,a,1,b,1)

    var normA = 0.0
    var normB = 0.0

    for( i <- 0 until a.size){
      if(a(i) != 0)
        normA += pow(a(i),2)

      if(b(i) != 0)
        normB += pow(b(i),2)
    }
    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0;
    else
      return dot_product/math.sqrt(normA*normB)
  }

  /** 计算余弦相似度 */
  def cosine(a : DenseVector, b : DenseVector) : Double = {
    if(a.size != b.size)
      return 0
    val al = a.toArray
    val bl = b.toArray

    val dot_product = CosineHashVector.f2jBLAS.ddot(a.size,al,1,bl,1)

    var normA = 0.0
    var normB = 0.0

    for( i <- 0 until al.size){
      if(al(i) != 0)
        normA += pow(al(i),2)

      if(bl(i) != 0)
        normB += pow(bl(i),2)
    }

    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0;
    else
      return dot_product/math.sqrt(normA*normB)
  }

  def one_vector(denseVector: DenseVector) : Array[Double] = {
    var r : Double = 0.0
    for(ele <- denseVector.values){
      r += ele * ele
    }
    if(r == 0)
      return denseVector.toArray
    val rs = Math.sqrt(r)
    var array = new Array[Double](denseVector.size)
    for( i <- 0 until denseVector.size){
      array(i) = denseVector.values(i)/rs
    }
    return array
  }

}
object CosineLSHForIncr{
  def cosine(a : DenseVector, b : DenseVector) : Double = {
    if(a.size != b.size)
      return 0
    val al = a.toArray
    val bl = b.toArray

    val dot_product = CosineHashVector.f2jBLAS.ddot(a.size,al,1,bl,1)

    var normA = 0.0
    var normB = 0.0

    for( i <- 0 until al.size){
      if(al(i) != 0)
        normA += pow(al(i),2)

      if(bl(i) != 0)
        normB += pow(bl(i),2)
    }

    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0
    else
      return dot_product/math.sqrt(normA*normB)
  }

  def cosine(a : SparseVector, b : SparseVector) : Double = {
    val al = a.indices.toList
    val bl = b.indices.toList
    val l = al.union(bl).distinct
    var normA = 0.0
    var normB = 0.0
    var dot_product = 0.0
    val mapa = al.zip(a.values).toMap
    val mapb = bl.zip(b.values).toMap
    for(ele <- l){
      if(mapa.contains(ele) && mapb.contains(ele))
        dot_product += mapa.get(ele).get * mapb.get(ele).get

      if(mapa.contains(ele))
        normA += mapa.get(ele).get * mapa.get(ele).get

      if(mapb.contains(ele))
        normB += mapb.get(ele).get * mapb.get(ele).get
    }
    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0;
    else
      return dot_product/math.sqrt(normA*normB)
  }
}