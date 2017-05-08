package org.distdb.mysql.server

import java.nio.charset.Charset
import io.netty.handler.codec.MessageToByteEncoder
import com.github.mauricio.async.db.mysql.message.client.ClientMessage
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import com.github.mauricio.async.db.mysql.message.server.ServerMessage
import com.github.mauricio.async.db.util.Log
import com.github.mauricio.async.db.mysql.decoder.HandshakeV10Decoder
import com.github.mauricio.async.db.mysql.decoder.OkDecoder
import com.github.mauricio.async.db.mysql.decoder.ErrorDecoder
import com.github.mauricio.async.db.mysql.decoder.ResultSetRowDecoder
import com.github.mauricio.async.db.mysql.decoder.EOFMessageDecoder
import com.github.mauricio.async.db.mysql.decoder.EOFMessageDecoder
import com.github.mauricio.async.db.mysql.decoder.ColumnDefinitionDecoder
import com.github.mauricio.async.db.mysql.codec.DecoderRegistry
import com.github.mauricio.async.db.mysql.decoder.QueryResultDecoder
import com.github.mauricio.async.db.mysql.decoder.ColumnProcessingFinishedDecoder
import com.github.mauricio.async.db.mysql.decoder.ColumnProcessingFinishedDecoder
import com.github.mauricio.async.db.exceptions.EncoderNotAvailableException
import scala.annotation.switch
import com.github.mauricio.async.db.util.ByteBufferUtils
import com.github.mauricio.async.db.util.BufferDumper
import io.netty.handler.codec.MessageToMessageEncoder

object MySQLServerEncoder {
  val log = Log.get[MySQLServerEncoder]
}


class MySQLServerEncoder(charset: Charset) extends MessageToMessageEncoder[ServerMessage] {
  
  import MySQLServerEncoder.log

  private final val handshakeEncoder = new HandshakeV10Decoder(charset)
  private final val okEncoder = new OkDecoder(charset)
  private final val errorEncoder = new ErrorDecoder(charset)
  private final val rsEncoder = new ResultSetRowDecoder(charset)
  private final val eofEncoder = EOFMessageDecoder
  private final val columnEncoder = new ColumnDefinitionDecoder(charset, new DecoderRegistry(charset))
  private final val columnFinishedEncoder = ColumnProcessingFinishedDecoder
  private final val qrEncoder = new QueryResultDecoder
  
  private var sequence = 1
  
  def encode(ctx: ChannelHandlerContext, message: ServerMessage, out: java.util.List[Object]): Unit = {
    var afterSequence = sequence
    val encoder = (message.kind: @switch) match {
      case ServerMessage.ServerProtocolVersion => {
        sequence = 0
        afterSequence = 2 
        this.handshakeEncoder
      }
      case ServerMessage.Error => {
        afterSequence = 1
        this.errorEncoder
      }
      case ServerMessage.Ok => {        
        afterSequence = 1
        this.okEncoder
      }
      case ServerMessage.EOF => {        
        afterSequence = 1
        this.eofEncoder
      }
      case ServerMessage.Row => {
        afterSequence += 1
        this.rsEncoder
      }
      
      case ServerMessage.ColumnDefinition => {
        afterSequence += 1
        this.columnEncoder
      }
      case ServerMessage.ColumnDefinitionFinished => {
        afterSequence += 1
        this.columnFinishedEncoder
      }
      case ServerMessage.QueryResult => {
          afterSequence += 1
         this.qrEncoder   
      }
      
      case _ => throw new EncoderNotAvailableException(message)
    }

    val result: ByteBuf = encoder.encode(message)

    ByteBufferUtils.writePacketLength(result, sequence)
    
    

    if ( log.isTraceEnabled ) {
      log.trace(s"Writing message ${message.getClass.getName} - \n${BufferDumper.dumpAsHex(result)}")
    }
    val r = result.readableBytes()
    
    out.add(result) 
    sequence = afterSequence 
  }
}

