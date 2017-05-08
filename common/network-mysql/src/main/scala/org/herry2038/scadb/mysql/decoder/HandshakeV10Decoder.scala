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
import org.herry2038.scadb.mysql.encoder.auth.AuthenticationMethod
import org.herry2038.scadb.mysql.message.server.{HandshakeMessage, ServerMessage}
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import io.netty.buffer.ByteBuf
import io.netty.util.CharsetUtil
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.mysql.encoder.auth.AuthenticationMethod
import org.herry2038.scadb.mysql.message.server.{HandshakeMessage, ServerMessage}
import org.herry2038.scadb.util.Log

object HandshakeV10Decoder {
  final val log = Log.get[HandshakeV10Decoder]
  final val SeedSize = 8
  final val SeedComplementSize = 12
  final val Padding = 10
  final val ASCII = CharsetUtil.US_ASCII
}

class HandshakeV10Decoder(charset: Charset) extends MessageDecoder {

  import org.herry2038.scadb.mysql.decoder.HandshakeV10Decoder._
  import org.herry2038.scadb.mysql.util.MySQLIO._

  def decode(buffer: ByteBuf): ServerMessage = {

    val serverVersion = buffer.readCString(ASCII)
    val connectionId = buffer.readUnsignedInt()

    var seed = new Array[Byte](SeedSize + SeedComplementSize)
    buffer.readBytes(seed, 0, SeedSize)

    buffer.readByte() // filler

    // read capability flags (lower 2 bytes)
    var serverCapabilityFlags = buffer.readUnsignedShort()

    /* New protocol with 16 bytes to describe server characteristics */
    // read character set (1 byte)
    val characterSet = buffer.readByte() & 0xff
    // read status flags (2 bytes)
    val statusFlags = buffer.readUnsignedShort()

    // read capability flags (upper 2 bytes)
    serverCapabilityFlags |= buffer.readUnsignedShort() << 16

    var authPluginDataLength = 0
    var authenticationMethod = AuthenticationMethod.Native

    if ((serverCapabilityFlags & CLIENT_PLUGIN_AUTH) != 0) {
      // read length of auth-plugin-data (1 byte)
      authPluginDataLength = buffer.readByte() & 0xff
    } else {
      // read filler ([00])
      buffer.readByte()
    }

    // next 10 bytes are reserved (all [00])
    buffer.readerIndex(buffer.readerIndex() + Padding)

    log.debug(s"Auth plugin data length was ${authPluginDataLength}")

    if ((serverCapabilityFlags & CLIENT_SECURE_CONNECTION) != 0) {
      val complement = if ( authPluginDataLength > 0 ) {
        authPluginDataLength - 1 - SeedSize
      } else {
        SeedComplementSize
      }

      buffer.readBytes(seed, SeedSize, complement)
      buffer.readByte()
    }

    if ((serverCapabilityFlags & CLIENT_PLUGIN_AUTH) != 0) {
      authenticationMethod = buffer.readUntilEOF(ASCII)
    }

    val message = new HandshakeMessage(
      serverVersion,
      connectionId,
      seed,
      serverCapabilityFlags,
      characterSet = characterSet,
      statusFlags = statusFlags,
      authenticationMethod = authenticationMethod
    )

    log.debug(s"handshake message was ${message}")

    message
  }
  
  def encode(message: ServerMessage): ByteBuf = {
    val hsm = message.asInstanceOf[HandshakeMessage]
    val buffer = ByteBufferUtils.packetBuffer()
    buffer.writeByte(10) //  protocol version 10
    buffer.writeBytes(hsm.serverVersion.getBytes)
    buffer.writeByte(0)    
    buffer.writeInt(hsm.connectionId.asInstanceOf[Int])
    buffer.writeBytes(hsm.seed,0,SeedSize)
    buffer.writeByte(0) // filter , always 0x00
    buffer.writeShort((hsm.serverCapabilities & 0xffff))
    buffer.writeByte(hsm.characterSet)
    buffer.writeShort(hsm.statusFlags)
    buffer.writeShort((hsm.serverCapabilities >> 16) & 0xffff)
    buffer.writeByte(21)  // scramble length , always 20
    buffer.writeZero(10)  // filter _ 10 reserved    
    buffer.writeBytes(hsm.seed,SeedSize,SeedComplementSize)
    buffer.writeZero(1)
    buffer
  }

}