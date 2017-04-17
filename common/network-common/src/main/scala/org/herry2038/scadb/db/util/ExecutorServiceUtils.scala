
package org.herry2038.scadb.db.util

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.ExecutionContext

object ExecutorServiceUtils {
  implicit val CachedThreadPool = Executors.newCachedThreadPool(DaemonThreadsFactory("db-async-default"))
  implicit val CachedExecutionContext = ExecutionContext.fromExecutor( CachedThreadPool )

  def newFixedPool( count : Int, name: String ) : ExecutorService = {
    Executors.newFixedThreadPool( count, DaemonThreadsFactory(name) )
  }

}
