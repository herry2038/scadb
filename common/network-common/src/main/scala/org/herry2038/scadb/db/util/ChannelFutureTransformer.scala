

package org.herry2038.scadb.db.util

import io.netty.channel.{ChannelFutureListener, ChannelFuture}
import org.herry2038.scadb.db.exceptions.CanceledChannelFutureException
import scala.concurrent.{Promise, Future}
import scala.language.implicitConversions

object ChannelFutureTransformer {

  implicit def toFuture(channelFuture: ChannelFuture): Future[ChannelFuture] = {
    val promise = Promise[ChannelFuture]

    channelFuture.addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if ( future.isSuccess ) {
          promise.success(future)
        } else {
          val exception = if ( future.cause == null ) {
            new CanceledChannelFutureException(future)
              .fillInStackTrace()
          } else {
            future.cause
          }

          promise.failure(exception)

        }
      }
    })

    promise.future
  }

}
