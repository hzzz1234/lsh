package com.dianping.search.offline.algorithm.e2lsh

import java.util

/**
  * Created by zhen.huaz on 2017/8/18.
  */
class TableNode (index : Int){
  val buckets = new util.HashMap[Int,List[String]]
}
