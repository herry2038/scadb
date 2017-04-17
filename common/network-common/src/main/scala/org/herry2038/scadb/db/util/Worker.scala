
package org.herry2038.scadb.db.util

import java.util.concurrent.ExecutorService
import scala.concurrent.{ExecutionContextExecutorService, ExecutionContext}

object Worker {
  val log = Log.get[Worker]

  def apply() : Worker = apply(ExecutorServiceUtils.newFixedPool(1, "db-async-worker"))

  def apply( executorService : ExecutorService ) : Worker = {
    new Worker(ExecutionContext.fromExecutorService( executorService ))
  }

}

class Worker( val executionContext : ExecutionContextExecutorService ) {

  import Worker.log

  def action(f: => Unit) {
    this.executionContext.execute(new Runnable {
      def run() {
        try {
          f
        } catch {
          case e : Exception => {
            log.error("Failed to execute task %s".format(f), e)
          }
        }
      }
    })
  }

  def shutdown {
    this.executionContext.shutdown()
  }

}
