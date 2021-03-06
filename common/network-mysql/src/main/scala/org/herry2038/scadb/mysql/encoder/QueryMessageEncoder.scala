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

package org.herry2038.scadb.mysql.encoder

import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import org.herry2038.scadb.mysql.message.client.{QueryMessage, ClientMessage}
import org.herry2038.scadb.db.util.ByteBufferUtils
import java.nio.charset.Charset

class QueryMessageEncoder( charset : Charset ) extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {

    val m = message.asInstanceOf[QueryMessage]
    val encodedQuery = m.query.getBytes( charset )
    val buffer = ByteBufferUtils.packetBuffer(4 + 1 + encodedQuery.length )
    buffer.writeByte( ClientMessage.Query )
    buffer.writeBytes( encodedQuery )

    buffer
  }
  
  def decode(buffer: ByteBuf): ClientMessage = {
    val t = buffer.readByte() // ClientMessage.Query
    if ( t == ClientMessage.Query ) {
      val query = buffer.readUntilEOF(charset)
      new QueryMessage(
        query
      )
    } else new ClientMessage(t)
  }
}