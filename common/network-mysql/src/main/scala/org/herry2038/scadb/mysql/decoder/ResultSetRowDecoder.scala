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

package org.herry2038.scadb.mysql.decoder

import java.nio.charset.Charset
import org.herry2038.scadb.mysql.message.server.{ResultSetRowMessage, ServerMessage}
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.util.ByteBufferUtils

object ResultSetRowDecoder {

  final val NULL = 0xfb

}

class ResultSetRowDecoder(charset: Charset) extends MessageDecoder {

  import org.herry2038.scadb.mysql.decoder.ResultSetRowDecoder.NULL

  def decode(buffer: ByteBuf): ServerMessage = {
    val row = new ResultSetRowMessage()

    while (buffer.isReadable()) {
      if (buffer.getUnsignedByte(buffer.readerIndex()) == NULL) {
        buffer.readByte()
        row += null
      } else {
        val length = buffer.readBinaryLength.asInstanceOf[Int]
        row += buffer.readBytes(length)
      }
    }

    row
  }
  
  
  
  def encode(message: ServerMessage): ByteBuf = {
    val rsr = message.asInstanceOf[ResultSetRowMessage]
    val buffer = ByteBufferUtils.packetBuffer()
    rsr.map{
      col =>
      	if ( col == null ) buffer.writeByte(NULL) 
      	else {
      	  buffer.writeLength(col.readableBytes())
      	  buffer.writeBytes(col)
      	}
  	}    
    
    buffer
  }  
}
