
package org.herry2038.scadb.db.util

import java.util.concurrent.{ Executors, ThreadFactory }
import java.util.concurrent.atomic.AtomicInteger

case class DaemonThreadsFactory(name: String) extends ThreadFactory {

  private val threadNumber = new AtomicInteger(1)

  def newThread(r: Runnable): Thread = {
    val thread = Executors.defaultThreadFactory().newThread(r)
    thread.setDaemon(true)
    val threadName = name + "-thread-" + threadNumber.getAndIncrement
    thread.setName(threadName)
    thread
  }
}
