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
package org.herry2038.scadb.mysql.server

import io.netty.channel._
import io.netty.handler.codec.CodecException
import org.herry2038.scadb.util.Log

import scala.annotation.switch
import scala.concurrent._
import org.herry2038.scadb.mysql.util.CharsetMapper
import java.nio.charset.Charset
import org.herry2038.scadb.mysql.message.client.ClientMessage
import org.herry2038.scadb.mysql.message.client.QueryMessage
import org.herry2038.scadb.mysql.message.server.{OkMessage, HandshakeMessage, AuthenticationSwitchRequest, ServerMessage}
import org.herry2038.scadb.db.util.ChannelFutureTransformer.toFuture
import org.herry2038.scadb.db.exceptions.DatabaseException
import org.herry2038.scadb.db.util.ExecutorServiceUtils
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.SocketChannel
import org.herry2038.scadb.mysql.message.client.HandshakeResponseMessage

class MySQLServerConnectionHandler (
							  charset : Charset,
                              charsetMapper: CharsetMapper,
                              handlerDelegate: MySQLServerHandlerDelegate,                                                           
                              executionContext : ExecutionContext
                              )
  extends SimpleChannelInboundHandler[Object] {
  private implicit val internalPool = executionContext
  
  import MySQLServerConnectionHandler.log
  
  private final val decoder = new MySQLServerDecoder(charset, charsetMapper)
  private final val encoder = new MySQLServerEncoder(charset)
  private final val queryResultEncode = new MySQLServerEncodeQueryResult(charset)
  private final val sysResultEncode = new MySQLServerEncodeSysResult(charset)
  private final val multiResultEncode = new MySQLServerEncodeMultiResults(charset)
  
  private var currentContext: ChannelHandlerContext = null
  var isReadWriteSplit: Boolean = false

  var lastInsertId: Long = 0 // 20170320 用于支持 Last Insert Id

  def authenticated :Unit = {
    decoder.authentiateSuccess
  }
  
  override def channelRead0(ctx: ChannelHandlerContext, message: Object) {
    message match {
      case m: ClientMessage => {
        (m.kind: @switch) match {
          case ClientMessage.Query => {
            handlerDelegate.onQuery(m.asInstanceOf[QueryMessage].query)
          }
          case ClientMessage.Quit => {            
            handlerDelegate.onQuit()
          }
          case ClientMessage.ClientProtocolVersion => {
            handlerDelegate.onHandshakeResponse(m.asInstanceOf[HandshakeResponseMessage])
          }
          case ClientMessage.InitDb =>
            handlerDelegate.ok()
          case ClientMessage.Ping =>
            handlerDelegate.ok()
          case _ => {
            handlerDelegate.error(-1,s"Unsupported command [${m.kind}]" )
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
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    // unwrap CodecException if needed
    cause match {
      case t: CodecException => handleException(t.getCause)
      case _ =>  handleException(cause)
    }
  }  

  private def handleException(cause: Throwable) {    
    handlerDelegate.exceptionCaught(cause)
  }

  override def handlerAdded(ctx: ChannelHandlerContext) {
    this.currentContext = ctx
  } 

  // def write( message : HandshakeMessage ) : ChannelFuture = writeAndHandleError(message)
  

  //def write( message : AuthenticationSwitchRequest ) : ChannelFuture = writeAndHandleError(message)

  //def write( message: ServerMessage) : ChannelFuture = writeAndHandleError(message)
  
  def write( message : Any ) : ChannelFuture =  writeAndHandleError(message)
  
  def disconnect: ChannelFuture = this.currentContext.close()

  def isConnected : Boolean = {
    if ( this.currentContext != null && this.currentContext.channel() != null ) {
      this.currentContext.channel.isActive
    } else {
      false
    }
  }

  private def writeAndHandleError( message : Any ) : ChannelFuture =  {

    if ( this.currentContext.channel().isActive ) {
      val future = this.currentContext.writeAndFlush(message)
      // TODO: first use scala future ,will change to use netty api later
      future.onFailure {
        case e : Throwable => handleException(e)
      }

      future
    } else {
      throw new DatabaseException("This channel is not active and can't take messages")
    }
  }
}

object MySQLServerConnectionHandler {
  final val log = Log.getByName(s"[server-connection-handler]")
  def bind(b:ServerBootstrap,charset: Charset,delegateCreator:MySQLServerDelegateCreator) = {	 
	 b.childHandler(new ChannelInitializer[SocketChannel]() {
	   override def initChannel(channel: SocketChannel): Unit = {
	     val delegate = delegateCreator.createDelegate
	     val handler = new MySQLServerConnectionHandler(
	    		 charset,
	    		 CharsetMapper.Instance,
	    		 delegate,
	    		 ExecutorServiceUtils.CachedExecutionContext)
	     delegate.setHandler(handler)
	     channel.pipeline.addLast(
	    		 handler.decoder,
	    		 handler.encoder,
	    		 handler.queryResultEncode,
	    		 handler.sysResultEncode,
	    		 handler.multiResultEncode,
	    		 handler)
	    		 
      }
	 })
	     
	     
  }
}