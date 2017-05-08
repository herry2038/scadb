package org.distdb.mysql.server

import java.nio.ByteBuffer
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{Unpooled, ByteBuf, ByteBufAllocator}
import io.netty.channel._
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.CodecException
import java.net.InetSocketAddress
import scala.Some
import scala.annotation.switch
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent._
import com.github.mauricio.async.db.Configuration
import com.github.mauricio.async.db.util.Log
import com.github.mauricio.async.db.mysql.util.CharsetMapper
import java.nio.charset.Charset
import com.github.mauricio.async.db.mysql.message.client.ClientMessage
import com.github.mauricio.async.db.mysql.message.client.QueryMessage
import com.github.mauricio.async.db.mysql.message.server.{OkMessage, HandshakeMessage, AuthenticationSwitchRequest, ServerMessage}
import com.github.mauricio.async.db.util.ChannelFutureTransformer.toFuture
import com.github.mauricio.async.db.exceptions.DatabaseException
import com.github.mauricio.async.db.util.ExecutorServiceUtils
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.SocketChannel
import com.github.mauricio.async.db.mysql.message.client.HandshakeResponseMessage

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