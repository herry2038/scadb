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

import org.herry2038.scadb.db.util.ByteBufferUtils.read3BytesInt
import org.herry2038.scadb.db.util.BufferDumper
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.exceptions.NegativeMessageSizeException
import java.util.concurrent.atomic.AtomicInteger
import org.herry2038.scadb.mysql.encoder.MessageEncoder
import java.nio.charset.Charset
import org.herry2038.scadb.mysql.util.CharsetMapper
import org.herry2038.scadb.mysql.encoder.HandshakeResponseEncoder
import org.herry2038.scadb.mysql.encoder.QueryMessageEncoder
import org.herry2038.scadb.util.Log

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