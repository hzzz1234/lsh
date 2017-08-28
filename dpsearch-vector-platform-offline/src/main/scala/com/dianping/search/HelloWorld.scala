package com.dianping.search

/**
 * Hello world!
 *
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    var myVar : String = "Foo";
    val myConst : String = "Foo";
    println(myVar)
    println(myConst)
    myVar = "FOOL";
    var mul = (x: Int, y: Int) => x*y
    println(mul(10,10))
    val set = Set(1,2,3)
    println(set.getClass.getName) //

    println(set.exists(_ % 100 == 0)) //true
  }
}
