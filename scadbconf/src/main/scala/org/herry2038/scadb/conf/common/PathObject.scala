package org.herry2038.scadb.conf.common

import org.jboss.netty.util.internal.ConcurrentHashMap

/**
 * Created by Administrator on 2017/5/31.
 */
trait PathObject[T <: SubObject] {
  val objects = new ConcurrentHashMap[String, T]
  def create(path: String, key: String): T
  def addKey(path: String, key: String): Unit = objects.put(key, create(path, key))
  def uptKey(path: String, key: String): Unit = {
    if ( objects.get(key) == null ) {
      objects.put(key, create(path, key))
    }
  }

  def delKey(path: String, key: String): Unit = {
    objects.get(key) match {
      case o if o != null =>
        o.close
        objects.remove(key)
      case _ =>
    }
  }
}
