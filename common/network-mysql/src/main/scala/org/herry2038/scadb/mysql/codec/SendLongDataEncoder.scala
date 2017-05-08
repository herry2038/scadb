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

import org.herry2038.scadb.mysql.message.client.{ClientMessage, SendLongDataMessage}
import org.herry2038.scadb.db.util.ByteBufferUtils
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import org.herry2038.scadb.mysql.message.client.{ClientMessage, SendLongDataMessage}
import org.herry2038.scadb.util.Log

object SendLongDataEncoder {
  val log = Log.get[SendLongDataEncoder]

  val LONG_THRESHOLD = 1023
}

class SendLongDataEncoder
    extends MessageToMessageEncoder[SendLongDataMessage](classOf[SendLongDataMessage]) {

  import org.herry2038.scadb.mysql.codec.SendLongDataEncoder.log

  def encode(ctx: ChannelHandlerContext, message: SendLongDataMessage, out: java.util.List[Object]): Unit = {
    if ( log.isTraceEnabled ) {
      log.trace(s"Writing message ${message.toString}")
    }

    val sequence = 0

    val headerBuffer = ByteBufferUtils.mysqlBuffer(3 + 1 + 1 + 4 + 2)
    ByteBufferUtils.write3BytesInt(headerBuffer, 1 + 4 + 2 + message.value.readableBytes())
    headerBuffer.writeByte(sequence)

    headerBuffer.writeByte(ClientMessage.PreparedStatementSendLongData)
    headerBuffer.writeBytes(message.statementId)
    headerBuffer.writeShort(message.paramId)

    val result = Unpooled.wrappedBuffer(headerBuffer, message.value)

    out.add(result)
  }

}
