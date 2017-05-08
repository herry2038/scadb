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

import io.netty.buffer.ByteBuf
import org.herry2038.scadb.mysql.message.server.{ErrorMessage, ServerMessage}
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import java.nio.charset.Charset
import org.herry2038.scadb.mysql.message.server.{ErrorMessage, ServerMessage}

import scala.language.implicitConversions
import org.herry2038.scadb.db.util.ByteBufferUtils
class ErrorDecoder( charset : Charset ) extends MessageDecoder {

  def decode(buffer: ByteBuf): ServerMessage = {
    val errCode = buffer.readShort()
    val b = buffer.readByte()
    buffer.readerIndex(buffer.readerIndex - 1)
    val sqlState = if ( b == '#' ) buffer.readFixedString(6, charset) else null

    new ErrorMessage(
      errCode,
      sqlState,
      buffer.readUntilEOF(charset)
    )

  }
  
  def encode(message: ServerMessage): ByteBuf = {
    val err = message.asInstanceOf[ErrorMessage]
    val buffer = ByteBufferUtils.packetBuffer()
    buffer.writeByte(0xff)
    buffer.writeShort(err.errorCode)
    if ( err.sqlState != null ) {
      buffer.writeBytes(err.sqlState.getBytes)
    }
    buffer.writeBytes(err.errorMessage.getBytes)
    buffer
  }

}
