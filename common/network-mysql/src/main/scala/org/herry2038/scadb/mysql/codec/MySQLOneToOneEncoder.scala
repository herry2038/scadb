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

import org.herry2038.scadb.db.exceptions.EncoderNotAvailableException
import org.herry2038.scadb.mysql.binary.BinaryRowEncoder
import org.herry2038.scadb.mysql.encoder._
import org.herry2038.scadb.mysql.message.client.ClientMessage
import org.herry2038.scadb.mysql.util.CharsetMapper
import org.herry2038.scadb.db.util.{BufferDumper, ByteBufferUtils}
import java.nio.charset.Charset
import org.herry2038.scadb.util.Log

import scala.annotation.switch
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.buffer.ByteBuf

object MySQLOneToOneEncoder {
  val log = Log.get[MySQLOneToOneEncoder]
}

class MySQLOneToOneEncoder(charset: Charset, charsetMapper: CharsetMapper)
    extends MessageToMessageEncoder[ClientMessage](classOf[ClientMessage]) {

  import MySQLOneToOneEncoder.log

  private final val handshakeResponseEncoder = new HandshakeResponseEncoder(charset, charsetMapper)
  private final val queryEncoder = new QueryMessageEncoder(charset)
  private final val rowEncoder = new BinaryRowEncoder(charset)
  private final val prepareEncoder = new PreparedStatementPrepareEncoder(charset)
  private final val executeEncoder = new PreparedStatementExecuteEncoder(rowEncoder)
  private final val authenticationSwitchEncoder = new AuthenticationSwitchResponseEncoder(charset)


  private var sequence = 1

  def encode(ctx: ChannelHandlerContext, message: ClientMessage, out: java.util.List[Object]): Unit = {
    val encoder = (message.kind: @switch) match {
      case ClientMessage.ClientProtocolVersion => this.handshakeResponseEncoder
      case ClientMessage.Quit => {
        sequence = 0
        QuitMessageEncoder
      }
      case ClientMessage.Query => {
        sequence = 0
        this.queryEncoder
      }
      case ClientMessage.PreparedStatementExecute => {
        sequence = 0
        this.executeEncoder
      }
      case ClientMessage.PreparedStatementPrepare => {
        sequence = 0
        this.prepareEncoder
      }
      case ClientMessage.AuthSwitchResponse => {
        sequence += 1
        this.authenticationSwitchEncoder
      }
      case ClientMessage.LoadData => {
        if ( sequence < 2 ) sequence = 2
        LoadDataMessageEncoder
      }
      case _ => throw new EncoderNotAvailableException(message)
    }

    val result: ByteBuf = encoder.encode(message)

    ByteBufferUtils.writePacketLength(result, sequence)

    sequence += 1

    if ( log.isTraceEnabled ) {
      log.trace(s"Writing message ${message.getClass.getName} - \n${BufferDumper.dumpAsHex(result)}")
    }

    out.add(result)
  }

}
