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

import java.nio.charset.Charset
import io.netty.handler.codec.MessageToByteEncoder
import org.herry2038.scadb.mysql.message.client.ClientMessage
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.mysql.message.server.ServerMessage
import org.herry2038.scadb.mysql.decoder.HandshakeV10Decoder
import org.herry2038.scadb.mysql.decoder.OkDecoder
import org.herry2038.scadb.mysql.decoder.ErrorDecoder
import org.herry2038.scadb.mysql.decoder.ResultSetRowDecoder
import org.herry2038.scadb.mysql.decoder.EOFMessageDecoder
import org.herry2038.scadb.mysql.decoder.EOFMessageDecoder
import org.herry2038.scadb.mysql.decoder.ColumnDefinitionDecoder
import org.herry2038.scadb.mysql.codec.DecoderRegistry
import org.herry2038.scadb.mysql.decoder.QueryResultDecoder
import org.herry2038.scadb.mysql.decoder.ColumnProcessingFinishedDecoder
import org.herry2038.scadb.mysql.decoder.ColumnProcessingFinishedDecoder
import org.herry2038.scadb.db.exceptions.EncoderNotAvailableException
import org.herry2038.scadb.util.Log
import scala.annotation.switch
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.db.util.BufferDumper
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

