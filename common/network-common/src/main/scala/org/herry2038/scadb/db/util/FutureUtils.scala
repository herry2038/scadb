
package org.herry2038.scadb.db.util

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object FutureUtils {

  def awaitFuture[T]( future : Future[T] ) : T = {
    Await.result(future, 5 seconds )
  }

}
