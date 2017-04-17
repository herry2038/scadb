
package org.herry2038.scadb.db.util

import org.slf4j.LoggerFactory

object Log {

  def get[T](implicit tag: reflect.ClassTag[T]) = {
    LoggerFactory.getLogger(tag.runtimeClass.getName)
  }

  def getByName(name: String) = {
    LoggerFactory.getLogger(name)
  }

}
