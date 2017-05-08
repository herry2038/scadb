//=========================================================================\\
//     _____               _ _
//    / ____|             | | |
//   | (___   ___ __ _  __| | |__
//    \___ \ / __/ _` |/ _` | '_ \
//    ____) | (_| (_| | (_| | |_) |
//   |_____/ \___\__,_|\__,_|_.__/

// Copyright 2016 The Scadb Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//=========================================================================\\
package org.herry2038.scadb.mysql

import org.herry2038.scadb.db._
import org.herry2038.scadb.db.exceptions._
import org.herry2038.scadb.mysql.codec.{MessageTrack, MySQLHandlerDelegate, MySQLConnectionHandler}
import org.herry2038.scadb.mysql.exceptions.MySQLException
import org.herry2038.scadb.mysql.message.client._
import org.herry2038.scadb.mysql.message.server._
import org.herry2038.scadb.mysql.util.CharsetMapper
import org.herry2038.scadb.db.util.ChannelFutureTransformer.toFuture
import org.herry2038.scadb.db.util._
import java.util.concurrent.atomic.{AtomicLong,AtomicReference}
import org.herry2038.scadb.util.Log

import scala.concurrent.{ExecutionContext, Promise, Future}
import io.netty.channel.{EventLoopGroup, ChannelHandlerContext}
import scala.util.Failure
import scala.util.Success

object MySQLConnection {
  final val Counter = new AtomicLong()
  final val MicrosecondsVersion = Version(5,6,0)
  final val log = Log.get[MySQLConnection]
}

class MySQLConnection(
                       val objId: Long,
                       configuration: Configuration,
                       charsetMapper: CharsetMapper = CharsetMapper.Instance,
                       group : EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
                       executionContext : ExecutionContext = ExecutorServiceUtils.CachedExecutionContext
                       )
  extends MySQLHandlerDelegate
  with Connection
{

  import MySQLConnection.log

  // validate that this charset is supported
  charsetMapper.toInt(configuration.charset)


  private final val connectionCount = MySQLConnection.Counter.incrementAndGet()
  private final val connectionId = s"[mysql-connection-$connectionCount]"
  private implicit val internalPool = executionContext

  private final val connectionHandler = new MySQLConnectionHandler(
    configuration,
    charsetMapper,
    this,
    group,
    executionContext,
    connectionId)

  private final val connectionPromise = Promise[Connection]()
  private final val disconnectionPromise = Promise[Connection]()

  private val queryPromiseReference = new AtomicReference[Option[Promise[QueryResult]]](None)
  private var connected = false
  private var _lastException : Throwable = null
  private var serverVersion : Version = null

  def version = this.serverVersion
  def lastException : Throwable = this._lastException
  def count : Long = this.connectionCount

  def connect: Future[Connection] = {
    this.connectionHandler.connect.onFailure {
      case e => this.connectionPromise.tryFailure(e)
    }
    isAutoCommit = configuration.autoCommit
    this.connectionPromise.future
  }

  def close: Future[Connection] = {
    if ( this.isConnected ) {
      if (!this.disconnectionPromise.isCompleted) {
        val exception = new DatabaseException("Connection is being closed")
        exception.fillInStackTrace()
        this.failQueryPromise(exception)
        this.connectionHandler.clearQueryState
        this.connectionHandler.write(QuitMessage.Instance).onComplete {
          case Success(channelFuture) => {
            this.connectionHandler.disconnect.onComplete {
              case Success(closeFuture) => this.disconnectionPromise.trySuccess(this)
              case Failure(e) => this.disconnectionPromise.tryFailure(e)
            }
          }
          case Failure(exception) => this.disconnectionPromise.tryFailure(exception)
        }
      }
    }

    this.disconnectionPromise.future
  }

  override def connected(ctx: ChannelHandlerContext) {
    log.debug("Connected to {}", ctx.channel.remoteAddress)
    this.connected = true
  }

  override def exceptionCaught(throwable: Throwable) {
    log.debug("Transport failure ", throwable)
    setException(throwable)
  }

  override def onError(message: ErrorMessage) {
    if ( !this.connectionPromise.isCompleted ) {  // 添加日期：20160302 原因：连接失败，需要释放到外边
      log.debug("Connected to database")
      this.connectionPromise.failure(new RuntimeException(message.errorMessage))
    } else {
      log.error("Received an error message -> {}", message)
      val exception = new MySQLException(message)
      exception.fillInStackTrace()
      this.setException(exception)
    }
  }

  /*
  override def setAutoCommit(b: Boolean) : Future[QueryResult] = {
    if ( isAutoCommit == b ) future { new QueryResult(0,null) }
    else {
      sendQuery("set autocommit=%d".format(if ( b ) 1 else 0)).andThen {  isAutoCommit = b   }
    }
  }
  */

  private def setException( t : Throwable ) {
    this._lastException = t
    this.connectionPromise.tryFailure(t)
    this.failQueryPromise(t)
  }

  override def onOk(message: OkMessage) {
    if ( !this.connectionPromise.isCompleted ) {
      log.debug("Connected to database")
      this.connectionPromise.success(this)
    } else {
      if (this.isQuerying) {
        this.succeedQueryPromise(
          new MySQLQueryResult(
            message.affectedRows,
            message.message,
            message.lastInsertId,
            message.statusFlags,
            message.warnings
          )
        )
      } else {
        log.warn("Received OK when not querying or connecting, not sure what this is")
      }
    }
  }

  def onEOF(message: EOFMessage) {
    if (this.isQuerying) {
      this.succeedQueryPromise(
        new MySQLQueryResult(
          0,
          null,
          -1,
          message.flags,
          message.warningCount
        )
      )
    }
  }

  override def onHandshake(message: HandshakeMessage) {
    this.serverVersion = Version(message.serverVersion)

    this.connectionHandler.write(new HandshakeResponseMessage(
      configuration.username,
      configuration.charset,
      message.seed,
      message.authenticationMethod,
      database = configuration.database,
      password = configuration.password
    ))
  }

  override def switchAuthentication( message : AuthenticationSwitchRequest ) {
    this.connectionHandler.write(new AuthenticationSwitchResponse( configuration.password, message ))
  }


  def sendQuery(query: String, handler: Any): Future[QueryResult] = {
    this.validateIsReadyForQuery()
    val promise = Promise[QueryResult]
    this.setQueryPromise(promise)
    if ( handler != null )
      this.connectionHandler.setMessageTrack(handler.asInstanceOf[MessageTrack])
    try {
      this.connectionHandler.write(new QueryMessage(query))
    } catch {
      case e: Throwable =>
        exceptionCaught(e)
    }
    promise.future
  }

  private def failQueryPromise(t: Throwable) {

    this.clearQueryPromise.foreach {
      _.tryFailure(t)
    }

  }

  private def succeedQueryPromise(queryResult: QueryResult) {

    this.clearQueryPromise.foreach {
      _.success(queryResult)
    }

  }

  def isQuerying: Boolean = this.queryPromise.isDefined

  def onResultSet(resultSet: ResultSet, message: EOFMessage) {
    if (this.isQuerying) {
      this.succeedQueryPromise(
        new MySQLQueryResult(
          resultSet.size,
          null,
          -1,
          message.flags,
          message.warningCount,
          Some(resultSet)
        )
      )
    }
  }

  def disconnect: Future[Connection] = this.close

  def isConnected: Boolean = this.connectionHandler.isConnected


  def sendPreparedStatement(query: String, values: Seq[Any]): Future[QueryResult] = {
    this.validateIsReadyForQuery()
    val totalParameters = query.count( _ == '?')
    if ( values.length != totalParameters ) {
      throw new InsufficientParametersException(totalParameters, values)
    }
    val promise = Promise[QueryResult]
    this.setQueryPromise(promise)
    this.connectionHandler.sendPreparedStatement(query, values)
    promise.future
  }


  override def toString: String = {
    "%s(%s,%d)".format(this.getClass.getName, this.connectionId, this.connectionCount)
  }

  private def validateIsReadyForQuery() {
    if ( isQuerying ) {
      throw new ConnectionStillRunningQueryException(this.connectionCount, false)
    }
  }

  private def queryPromise: Option[Promise[QueryResult]] = queryPromiseReference.get()

  private def setQueryPromise(promise: Promise[QueryResult]) {
    if (!this.queryPromiseReference.compareAndSet(None, Some(promise)))
      throw new ConnectionStillRunningQueryException(this.connectionCount, true)
  }

  private def clearQueryPromise : Option[Promise[QueryResult]] = {
    this.queryPromiseReference.getAndSet(None)
  }

}
