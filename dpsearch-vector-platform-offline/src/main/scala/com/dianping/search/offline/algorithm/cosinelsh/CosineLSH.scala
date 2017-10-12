package com.dianping.search.offline.algorithm.cosinelsh

import java.lang.Math._

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
class CosineLSH (origin_data : RDD[(String,Vector)], dimension : Int, numRows : Int, numBands : Int, minClusterSize : Int) extends Serializable {

  /** run CosineLSH using the constructor parameters */
  def run() : CosineLSHModel = {

    //创建一个minhash的模型
    val model = new CosineLSHModel(dimension,numRows)

    val data = origin_data.map(line => (line._1,line._2.toDense)).cache
    // 计算签名矩阵
    // - 对向量hash numrows次
    // - 将hash后的值定位到对应的band中,后面会根据band号进行分组,构建局部签名的桶
    // output ((vector idx, band#), minhash)
    val signatures = data.flatMap(v => model.hashVectors.flatMap(h => List(((v._1, h._2 % numBands),h._1.cosinehash(v._2.toDense)))))

    // 对同band下的数据进行签名
    // output ((band+minlist)->bandhashvalue,key)
    val mid = signatures.groupByKey().map(x => (transforMD5(x._1._2,x._2), x._1._1)).cache()
    // output (key,hashlist)
    model.vector_hashlist = data.join(mid.map(x => x.swap).groupByKey().map(x => (x._1,x._2.toList))).map(row => (row._1,row._2._2)).cache()

    // 组织所有在同一个band且minhashlist的值一样的数据合并到一起
    // output (bandhashvalue, vectorid list)
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
    md5Hash(sb.toString)
  }
  /**
    *
    * @param text
    * @return
    */
  def md5Hash(text:String):String =
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_}.substring(8,24)


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
object CosineLSH{
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
  def main(args: Array[String]) {
    println(    java.security.MessageDigest.getInstance("MD5").digest("adasdad".getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_})
    var a = Vectors.dense(Array(-121.23303966327435,62.76273805843499,-45.074399034985895,-91.92558504014973,246.64505567900002,110.08260991777072,299.91658815582946,-78.37865077466509,-73.73963739111149,402.69494095834386,39.0352760472159,321.21898036689294,-247.07669629717827,-279.68284003425,-186.28159747271042,-91.14310380801454,-174.28862927833467,-347.3440873655236,-265.02299664214905,383.10815575020246,-513.9031796403893,149.87285262074138,270.9729954982624,-87.96116346617049,180.55173957471152,1.7083242438468806,-234.3967930069554,-302.35473414763874,-11.563527517380068,165.39449334223644,-24.532899823100408,110.26577297796754,215.35358435159728,-374.8211013413801,-306.93713886881017,-36.496549860559924,-15.87582781840505,-230.64413444668628,86.98599355024704,-51.33639858666682,-127.1434317509667,152.26688866782538,-84.92119254175842,-52.77397012084772,-337.3448191426358,166.75103126420157,208.3244487570046,193.027736944345,-337.52805401894784,-11.226055110171005,202.04443148401035,260.95762632477874,144.97997331808298,135.97262063352406,-5.194617954571186,60.061295310651886,127.48950522390518,145.145733046394,230.29999396617208,318.5588104050686,21.49665652626925,99.40449997772423,-19.389682124653824,-130.94842830579478,11.783906733640483,38.53030048665656,-31.501762535691142,77.05593955468692,104.13783833791888,64.69630795026487,9.808901184356802,-24.710921041404326,202.90857390602773,-192.4193708169251,-125.35916335697627,-179.69193345454232,183.1912260490078,141.79335183277,102.77011028126974,59.20367643111233,266.96719116349857,172.3956862663214,236.29302987303151,104.40617504857515,-73.45144137514076,-257.2831388241834,-179.861078119454,-62.172779307684664,-14.230409963875003,149.54423315382095,-109.84609031710895,-19.228178019816493,21.368583752476436,-117.88673933828238,-24.807851244354506,-285.2682035506486,107.54809227482716,-2.6981540863942017,145.42930030371622,-150.0599933434502)).toSparse
    var b = Vectors.dense(Array(1.2432785563301496,-5.48311567867246,-2.06181001155418,8.50390295784339,-30.825219557817327,27.114497615057882,-5.6620225037835095,-7.17891127416635,21.77386993107671,-21.757134368873043,2.7861735741643203,34.19477572039198,53.62317313962631,20.849376130318092,-22.989873929102075,37.71615464586654,12.11340239279204,5.32742356658439,2.8730451167748488,-20.126740200985008,-13.945264081253962,2.0161648524741613,-0.21478332639580028,-3.089003568519589,-5.471094198823989,-30.95481336495388,-34.35194564474766,-19.044253204531188,5.772835076295951,7.44038850051751,-0.8713744200191103,-2.6093558493144795,-15.87202442252829,2.55250734209433,-8.042961717516086,-20.000502871512428,0.9516438950338392,-13.917964478557751,20.65475075143339,3.962079754561389,-16.54843107754223,-8.143115161690408,6.9771938951624595,-24.428310018359724,-10.93407318898211,23.20831911413616,-7.5520269127464505,-16.015792146058796,12.217906208179198,6.7479592743065595,-8.7298201778609,2.8215521776075994,5.4967869963810685,-22.4975300996317,-14.404966760153638,-18.97029423478939,25.78967075604887,-16.15592706905677,-25.496784652039178,-23.307198824371202,-21.43450875449321,-1.3541242064332102,26.805338558009375,6.578737346412959,-5.430166797639979,10.31160359008347,23.068928369077966,0.5624119775409917,31.129855043292004,8.621777921724828,-8.80059380490767,38.117629550188795,-25.205088928552044,9.29508500459429,15.93711401364886,20.810637004922523,0.93353459929245,-8.859785994215038,-2.440431688120661,19.002009878799132,1.333129107959719,-18.542670483507536,-7.5444283234150715,-0.5077481813112289,10.227692351697371,3.429938833991891,-0.3803633782742807,-6.568761575128371,6.40868618510504,5.7538235780791585,-24.6022564667472,8.894786281371909,-12.039431476462571,-24.752297315061526,-5.437732957482099,15.026560726250827,0.8095127067009923,-10.05997913568447,-19.106762222207223,-31.01827975954829)).toSparse
    println(cosine(a,b))
    println(cosine(a.toDense,b.toDense))
  }
}