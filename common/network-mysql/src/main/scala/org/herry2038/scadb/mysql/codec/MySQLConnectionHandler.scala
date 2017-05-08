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
package org.herry2038.scadb.mysql.codec

import java.io.FileInputStream
import java.nio.ByteBuffer

import org.herry2038.scadb.db.Configuration
import org.herry2038.scadb.db.general.MutableResultSet
import org.herry2038.scadb.mysql.binary.BinaryRowDecoder
import org.herry2038.scadb.mysql.message.client._
import org.herry2038.scadb.mysql.message.server._
import org.herry2038.scadb.mysql.util.CharsetMapper
import org.herry2038.scadb.db.util.ChannelFutureTransformer.toFuture
import org.herry2038.scadb.db.util._
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{Unpooled, ByteBuf, ByteBufAllocator}
import io.netty.channel._
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.CodecException
import java.net.InetSocketAddress

import org.herry2038.scadb.mysql.message.client.{QuitMessage, AuthenticationSwitchResponse, HandshakeResponseMessage, QueryMessage}
import org.herry2038.scadb.mysql.message.server._
import org.herry2038.scadb.mysql.util.CharsetMapper
import org.herry2038.scadb.util.Log

import scala.annotation.switch
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent._
import org.herry2038.scadb.db.exceptions.DatabaseException


class MySQLConnectionHandler(
                              configuration: Configuration,
                              charsetMapper: CharsetMapper,
                              handlerDelegate: MySQLHandlerDelegate,
                              group : EventLoopGroup,
                              executionContext : ExecutionContext,
                              connectionId : String
                              )
  extends SimpleChannelInboundHandler[Object] {

  private implicit val internalPool = executionContext
  private final val log = Log.getByName(s"[connection-handler]${connectionId}")
  private final val bootstrap = {
    val b = new Bootstrap().group(this.group)
    b.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE,true)
    b
  }
  private final val connectionPromise = Promise[MySQLConnectionHandler]
  private final val decoder = new MySQLFrameDecoder(configuration.charset, connectionId, configuration.returnStr)
  private final val encoder = new MySQLOneToOneEncoder(configuration.charset, charsetMapper)
  private final val sendLongDataEncoder = new SendLongDataEncoder()
  private final val currentParameters = new ArrayBuffer[ColumnDefinitionMessage]()
  private final var currentColumns = new ArrayBuffer[ColumnDefinitionMessage]()
  private final val parsedStatements = new HashMap[String,PreparedStatementHolder]()
  private final val binaryRowDecoder = new BinaryRowDecoder()

  private var currentPreparedStatementHolder : PreparedStatementHolder = null
  private var currentPreparedStatement : PreparedStatement = null
  private var currentQuery : MutableResultSet[ColumnDefinitionMessage] = null
  private var currentContext: ChannelHandlerContext = null
  private var messageTrack: MessageTrack = null
  private var localInputStream : FileInputStream = null
  private final val LOAD_BUFFER_SIZE = 16384
  private final val loadBuffer = new Array[Byte](LOAD_BUFFER_SIZE)

  private var state : Int = 0
   // state 0 : connected , send hand shake, 1 : authenticated received 2 : autocommit sended 3 : charset sended
   // MySQLConnection.connected ==> state = 3


  def connect: Future[MySQLConnectionHandler] = {
    this.bootstrap.channel(classOf[NioSocketChannel])
    this.bootstrap.handler(new ChannelInitializer[io.netty.channel.Channel]() {

      override def initChannel(channel: io.netty.channel.Channel): Unit = {
        channel.pipeline.addLast(
          decoder,
          encoder,
          sendLongDataEncoder,
          MySQLConnectionHandler.this)
      }

    })

    this.bootstrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
    this.bootstrap.option[ByteBufAllocator](ChannelOption.ALLOCATOR, LittleEndianByteBufAllocator.INSTANCE)

    this.bootstrap.connect(new InetSocketAddress(configuration.host, configuration.port)).onFailure {
      case exception => this.connectionPromise.tryFailure(exception)
    }

    this.connectionPromise.future
  }

  override def channelRead0(ctx: ChannelHandlerContext, message: Object) {
    message match {
      case m: ServerMessage => {
        if ( state == 0) { // connected , authenticating
          (m.kind: @switch) match {
            case ServerMessage.ServerProtocolVersion => {
              handlerDelegate.onHandshake(m.asInstanceOf[HandshakeMessage])
            }
            case ServerMessage.Ok => {
              this.clearQueryState
              //handlerDelegate.onOk(m.asInstanceOf[OkMessage])
              state = 1
              writeAndHandleError(new QueryMessage(if ( configuration.autoCommit) "set autocommit=true" else "set autocommit=false"))
            }
            case ServerMessage.Error => {
              log.error("authenticated failed in state {}", state)
              handlerDelegate.onError(m.asInstanceOf[ErrorMessage])
              disconnect
            }
            case _ => {
              log.error("authenticated phase get a invalid msg {}", m)
              handlerDelegate.onError(new ErrorMessage(-1, "authenticated phase get a invalid msg."))
              disconnect
            }
          }
        } else if ( state == 1 || state == 2) {
          (m.kind: @switch) match {
            case ServerMessage.Ok => {
              this.clearQueryState
              if ( state == 1) {
                writeAndHandleError(new QueryMessage("set names '%s'".format(CharsetMapper.toString(configuration.charset,configuration.utf8mb4))))
              } else {
                handlerDelegate.onOk(m.asInstanceOf[OkMessage])
              }
              state += 1
            }
            case ServerMessage.Error => {
              log.error("authenticated failed in state {}", state)
              this.clearQueryState
              val msg = m.asInstanceOf[ErrorMessage]
              handlerDelegate.onError(new ErrorMessage(msg.errorCode,msg.sqlState, "in state " + state + ", get error:" + msg.errorMessage))
              disconnect
            }
          }
        } else if ( state == 3 ) {
          (m.kind: @switch) match {
            case ServerMessage.Ok => {
              val mt = this.messageTrack
              this.clearQueryState
              if ( mt == null ) {
                handlerDelegate.onOk(m.asInstanceOf[OkMessage])
              } else {
                mt.onOk(handlerDelegate, m.asInstanceOf[OkMessage])
              }
            }
            case ServerMessage.Error => {
              this.clearQueryState
              handlerDelegate.onError(m.asInstanceOf[ErrorMessage])
            }
            case ServerMessage.EOF => {
              this.handleEOF(m)
            }
            case ServerMessage.ColumnDefinition => {
              val message = m.asInstanceOf[ColumnDefinitionMessage]

              if (currentPreparedStatementHolder != null && this.currentPreparedStatementHolder.needsAny) {
                this.currentPreparedStatementHolder.add(message)
              }

              this.currentColumns += message
            }
            case ServerMessage.ColumnDefinitionFinished => {
              this.onColumnDefinitionFinished()
            }
            case ServerMessage.PreparedStatementPrepareResponse => {
              this.onPreparedStatementPrepareResponse(m.asInstanceOf[PreparedStatementPrepareResponse])
            }
            case ServerMessage.Row => {
              val message = m.asInstanceOf[ResultSetRowMessage]
              val items = new Array[Any](message.size)

              var x = 0
              while (x < message.size) {
                items(x) = if (message(x) == null) {
                  null
                } else {
                  val columnDescription = this.currentQuery.columnTypes(x)
                  columnDescription.textDecoder.decode(columnDescription, message(x), configuration.charset)
                }
                x += 1
              }

              if ( messageTrack == null ) {
                this.currentQuery.addRow(items)
              } else {
                this.messageTrack.onRow(items)
              }
            }
            case ServerMessage.BinaryRow => {
              val message = m.asInstanceOf[BinaryRowMessage]
              this.currentQuery.addRow(this.binaryRowDecoder.decode(message.buffer, this.currentColumns))
            }
            case ServerMessage.ParamProcessingFinished => {
            }
            case ServerMessage.ParamAndColumnProcessingFinished => {
              this.onColumnDefinitionFinished()
            }
            case ServerMessage.LoadLocal => {
              this.onLoadLocalStart(m.asInstanceOf[LoadLocalMsg].file)
            }
          }
        }
      }
    }

  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    log.debug("Channel became active")
    handlerDelegate.connected(ctx)
  }


  override def channelInactive(ctx: ChannelHandlerContext) {
    log.debug("Channel became inactive")
    super.channelInactive(ctx) // 添加： 2016-11-21
    handleException(new DatabaseException("Connection disconnected!"))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    // unwrap CodecException if needed
    cause match {
      case t: CodecException => handleException(t.getCause)
      case _ =>  handleException(cause)
    }

  }

  private def handleException(cause: Throwable) {
    if (!this.connectionPromise.isCompleted) {
      this.connectionPromise.failure(cause)
    }
    handlerDelegate.exceptionCaught(cause)
    try { if ( localInputStream != null ) localInputStream.close ; localInputStream = null } catch { case _: Throwable => }
  }

  override def handlerAdded(ctx: ChannelHandlerContext) {
    this.currentContext = ctx
  }

  def write( message : QueryMessage ) : ChannelFuture = {
    this.decoder.queryProcessStarted()
    writeAndHandleError(message)
  }

  def sendPreparedStatement( query: String, values: Seq[Any] )  {
    val preparedStatement = new PreparedStatement(query, values)

    this.currentColumns.clear()
    this.currentParameters.clear()

    this.currentPreparedStatement = preparedStatement

    this.parsedStatements.get(preparedStatement.statement) match {
      case Some( item ) => {
        this.executePreparedStatement(item.statementId, item.columns.size, preparedStatement.values, item.parameters)
      }
      case None => {
        decoder.preparedStatementPrepareStarted()
        writeAndHandleError( new PreparedStatementPrepareMessage(preparedStatement.statement) )
      }
    }
  }

  def write( message : HandshakeResponseMessage ) : ChannelFuture = {
    decoder.hasDoneHandshake = true
    writeAndHandleError(message)
  }

  def write( message : AuthenticationSwitchResponse ) : ChannelFuture = writeAndHandleError(message)

  def write( message : QuitMessage ) : ChannelFuture = {
    writeAndHandleError(message)
  }

  def disconnect: ChannelFuture = this.currentContext.close()

  def clearQueryState {
    // this.currentColumns.clear()  修改日志：20160307， 由于异步处理，这个参数我们还需要留待后续进一步处理，不能clear
    this.currentColumns = new ArrayBuffer[ColumnDefinitionMessage]()
    this.currentParameters.clear()
    this.currentQuery = null
    this.messageTrack = null
  }

  def setMessageTrack(mt: MessageTrack) = this.messageTrack = mt

  def isConnected : Boolean = {
    if ( this.currentContext != null && this.currentContext.channel() != null ) {
      this.currentContext.channel.isActive && state == 3
    } else {
      false
    }
  }

  private def executePreparedStatement( statementId : Array[Byte], columnsCount : Int, values : Seq[Any], parameters : Seq[ColumnDefinitionMessage] ) {
    decoder.preparedStatementExecuteStarted(columnsCount, parameters.size)
    this.currentColumns.clear()
    this.currentParameters.clear()

    var nonLongIndices: Set[Int] = Set()
    values.zipWithIndex.foreach {
      case (Some(value), index) if isLong(value) =>
        sendLongParameter(statementId, index, value)

      case (value, index) if isLong(value) =>
        sendLongParameter(statementId, index, value)

      case (_, index) =>
        nonLongIndices += index
    }

    writeAndHandleError(new PreparedStatementExecuteMessage(statementId, values, nonLongIndices, parameters))
  }

  private def isLong(value: Any): Boolean = {
    value match {
      case v : Array[Byte] => v.length > SendLongDataEncoder.LONG_THRESHOLD
      case v : ByteBuffer => v.remaining() > SendLongDataEncoder.LONG_THRESHOLD
      case v : ByteBuf => v.readableBytes() > SendLongDataEncoder.LONG_THRESHOLD

      case _ => false
    }
  }

  private def sendLongParameter(statementId: Array[Byte], index: Int, longValue: Any) {
    longValue match {
      case v : Array[Byte] =>
        sendBuffer(Unpooled.wrappedBuffer(v), statementId, index)

      case v : ByteBuffer =>
        sendBuffer(Unpooled.wrappedBuffer(v), statementId, index)

      case v : ByteBuf =>
        sendBuffer(v, statementId, index)
    }
  }

  private def sendBuffer(buffer: ByteBuf, statementId: Array[Byte], paramId: Int) {
    writeAndHandleError(new SendLongDataMessage(statementId, buffer, paramId))
  }

  private def onPreparedStatementPrepareResponse( message : PreparedStatementPrepareResponse ) {
    this.currentPreparedStatementHolder = new PreparedStatementHolder( this.currentPreparedStatement.statement, message)
  }

  def onColumnDefinitionFinished() {

    val columns = if ( this.currentPreparedStatementHolder != null ) {
      this.currentPreparedStatementHolder.columns
    } else {
      this.currentColumns
    }


    this.currentQuery = new MutableResultSet[ColumnDefinitionMessage](columns)

    if (this.currentPreparedStatementHolder != null) {
      this.parsedStatements.put(this.currentPreparedStatementHolder.statement, this.currentPreparedStatementHolder)
      this.executePreparedStatement(
        this.currentPreparedStatementHolder.statementId,
        this.currentPreparedStatementHolder.columns.size,
        this.currentPreparedStatement.values,
        this.currentPreparedStatementHolder.parameters
      )
      this.currentPreparedStatementHolder = null
      this.currentPreparedStatement = null
    }

    // 添加 messageTrack
    if ( messageTrack != null ) {
      messageTrack.onColumns(columns)
    }
  }

  private def writeAndHandleError( message : Any ) : ChannelFuture =  {

    if ( this.currentContext.channel().isActive ) {
      val future = this.currentContext.writeAndFlush(message)

      future.onFailure {
        case e : Throwable => handleException(e)
      }

      future
    } else {
      throw new DatabaseException("This channel is not active and can't take messages")
    }
  }

  private def handleEOF( m : ServerMessage ) {
    m match {
      case eof : EOFMessage => {
        val resultSet = this.currentQuery
        val mt = this.messageTrack
        this.clearQueryState
        if ( mt == null ) {
          if (resultSet != null) {
            handlerDelegate.onResultSet(resultSet, eof)
          } else {
            handlerDelegate.onEOF(eof)
          }
        } else {
          mt.onEof(handlerDelegate, eof)
        }

      }
      case authenticationSwitch : AuthenticationSwitchRequest => {
        handlerDelegate.switchAuthentication(authenticationSwitch)
      }
    }
  }
  private def onLoadLocalStart(file: String): Unit = {
    try {
      localInputStream = new FileInputStream(file)
      onLoadLocal(false)
    } catch {
      case ex: Exception =>
        log.error(s"open file ${file} error!", ex)
        onLoadLocal(true)
    }
  }

  private def onLoadLocal(finished: Boolean): Unit = {
    val size = if ( finished ) 0 else localInputStream.read(loadBuffer)
    if ( size <= 0 ) {
      try { if ( localInputStream != null ) localInputStream.close ; localInputStream = null } catch { case _: Throwable => }
    }

    val f = writeAndHandleError(new LoadLocalDataMessage(loadBuffer, if ( size < 0 ) 0 else size))
    if ( size > 0 )
      f.addListener(new ChannelFutureListener() {
        override def operationComplete(future: ChannelFuture) {
          if (future.isSuccess()) {
            onLoadLocal(false)
          } else {
          }
        }
      })
  }
}
