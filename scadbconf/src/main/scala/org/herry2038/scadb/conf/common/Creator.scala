package org.herry2038.scadb.conf.common

/**
 * Created by Administrator on 2017/5/31.
 */
//class Foo[A](implicit m: scala.reflect.Manifest[A]) {
//  def create: A = m.erasure.newInstance.asInstanceOf[A]
//}


class Creator[T](implicit manifest: reflect.Manifest[T]) {
  def create: T = manifest.runtimeClass.newInstance().asInstanceOf[T]

  def create(path: String, key: String): T = {
    val constructor = manifest.runtimeClass.getConstructor(classOf[String], classOf[String])
    constructor.newInstance(path, key).asInstanceOf[T]
  }
}