package org.herry2038.scadb.conf.common

/**
 * Created by Administrator on 2017/5/31.
 */
object TestCreator {
  def main(args: Array[String]) {
    val creator = new Creator[A]
    for ( c <- manifest[A].runtimeClass.getConstructors ) {
      println(c)
    }

    val aaa = creator.create("a", "b")
    println(aaa.a + "," + aaa.b)
  }
}
