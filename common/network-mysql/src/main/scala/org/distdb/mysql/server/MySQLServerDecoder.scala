package org.distdb.mysql.server

import com.github.mauricio.async.db.exceptions._
import com.github.mauricio.async.db.mysql.decoder._
import com.github.mauricio.async.db.mysql.message.server._
import com.github.mauricio.async.db.util.ByteBufferUtils.read3BytesInt
import com.github.mauricio.async.db.util.ChannelWrapper.bufferToWrapper
import com.github.mauricio.async.db.util.{BufferDumper, Log}
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import com.github.mauricio.async.db.exceptions.NegativeMessageSizeException
import java.util.concurrent.atomic.AtomicInteger
import com.github.mauricio.async.db.mysql.encoder.MessageEncoder
import com.github.mauricio.async.db.mysql.encoder.AuthenticationSwitchResponseEncoder
import java.nio.charset.Charset
import com.github.mauricio.async.db.mysql.util.CharsetMapper
import com.github.mauricio.async.db.mysql.encoder.HandshakeResponseEncoder
import com.github.mauricio.async.db.mysql.encoder.QueryMessageEncoder

class MySQLServerDecoder(charset: Charset, charsetMapper: CharsetMapper) extends ByteToMessageDecoder {
  private final val log = Log.getByName("MySQLServerDecoder")
  private final val messagesCount = new AtomicInteger()
  
  private var authenticated = false 
  private var isInQuery = false
  private var processingColumns = false
  private var processingParams = false
  
  private val handshakeResponseEncode = new HandshakeResponseEncoder(charset,charsetMapper)
  private val queryEncode = new QueryMessageEncoder(charset)
  
  private var totalParams = 0L
  private var processedParams = 0L
  private var totalColumns = 0L
  private var processedColumns = 0L
  
  def authentiateSuccess : Unit ={
      authenticated = true
  }
  
  def decode(ctx: ChannelHandlerContext, buffer: ByteBuf, out: java.util.List[Object]): Unit = {
    if (buffer.readableBytes() > 4) {

      buffer.markReaderIndex()

      val size = read3BytesInt(buffer)

      val sequence = buffer.readUnsignedByte() // we have to read this

      if (buffer.readableBytes() >= size) {

        messagesCount.incrementAndGet()

        val messageType = buffer.getByte(buffer.readerIndex())

        if (size < 0) {
          throw new NegativeMessageSizeException(messageType, size)
        }

        val slice = buffer.readSlice(size)

        if (log.isTraceEnabled) {
          log.trace(s"Reading message type $messageType - " +
            s"(count=$messagesCount,status=$authenticated,size=$size,isInQuery=$isInQuery,processingColumns=$processingColumns,processingParams=$processingParams,processedColumns=$processedColumns,processedParams=$processedParams)" +
            s"\n${BufferDumper.dumpAsHex(slice)}}")
        }

        //slice.readByte()

        val decoder = if ( authenticated ) this.handleCommonFlow(messageType,slice) else this.handshakeResponseEncode 
        
        this.doDecoding(decoder, slice, out)        
      } else {
        buffer.resetReaderIndex()
      }

    }
  }
  
  
  private def handleCommonFlow(messageType: Byte, slice: ByteBuf) = {
    queryEncode
  }
  
  private def doDecoding(decoder: MessageEncoder, slice: ByteBuf, out: java.util.List[Object]) {    
	val result = decoder.decode(slice)
	out.add(result)
  }
}