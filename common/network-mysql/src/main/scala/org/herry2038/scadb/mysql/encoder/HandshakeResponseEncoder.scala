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

import java.nio.charset.Charset

import org.herry2038.scadb.db.exceptions.UnsupportedAuthenticationMethodException
import org.herry2038.scadb.mysql.encoder.auth.AuthenticationMethod
import org.herry2038.scadb.mysql.message.client.{ClientMessage, HandshakeResponseMessage}
import org.herry2038.scadb.mysql.util.CharsetMapper
import org.herry2038.scadb.db.util.ByteBufferUtils
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import io.netty.util.CharsetUtil
import org.herry2038.scadb.util.Log

object HandshakeResponseEncoder {

  final val MAX_3_BYTES = 0x00ffffff
  final val PADDING: Array[Byte] = List.fill(23) {
    0.toByte
  }.toArray

  final val log = Log.get[HandshakeResponseEncoder]

}

class HandshakeResponseEncoder(charset: Charset, charsetMapper: CharsetMapper) extends MessageEncoder {

  import org.herry2038.scadb.mysql.encoder.HandshakeResponseEncoder._
  import org.herry2038.scadb.mysql.util.MySQLIO._

  private val authenticationMethods = AuthenticationMethod.Availables

  def encode(message: ClientMessage): ByteBuf = {

    val m = message.asInstanceOf[HandshakeResponseMessage]

    var clientCapabilities = 0

    clientCapabilities |=
      CLIENT_PLUGIN_AUTH |
      CLIENT_PROTOCOL_41 |
      CLIENT_TRANSACTIONS |
      CLIENT_MULTI_RESULTS |
      CLIENT_SECURE_CONNECTION |
      CLIENT_LOCAL_FILES

    if (m.database.isDefined) {
      clientCapabilities |= CLIENT_CONNECT_WITH_DB
    }

    val buffer = ByteBufferUtils.packetBuffer()

    buffer.writeInt(clientCapabilities)
    buffer.writeInt(MAX_3_BYTES)
    buffer.writeByte(charsetMapper.toInt(charset))
    buffer.writeBytes(PADDING)
    ByteBufferUtils.writeCString( m.username, buffer, charset )

    if ( m.password.isDefined ) {
      val method = m.authenticationMethod
      val authenticator = this.authenticationMethods.getOrElse(
        method, { throw new UnsupportedAuthenticationMethodException(method) })
      val bytes = authenticator.generateAuthentication(charset, m.password, m.seed)
      buffer.writeByte(bytes.length)
      buffer.writeBytes(bytes)
    } else {
      buffer.writeByte(0)
    }

    if ( m.database.isDefined ) {
      ByteBufferUtils.writeCString( m.database.get, buffer, charset )
    }

    ByteBufferUtils.writeCString( m.authenticationMethod, buffer, charset )

    buffer
  }

  def decode(buffer: ByteBuf): ClientMessage = {
    val clientCapabilities = buffer.readUnsignedInt().asInstanceOf[Int]
    buffer.readInt() // MAX_3_BYTES
    val cs = buffer.readByte() 
    buffer.readBytes(23) // padding
    
    val userName = buffer.readCString(charset)    
    val scramble = buffer.readLengthEncodedString(CharsetUtil.ISO_8859_1)
    val db = if ( (clientCapabilities & CLIENT_CONNECT_WITH_DB) != 0 ) 
    			Some(buffer.readCString(charset))
    		else None
    //val method = buffer.readCString(charset) 似乎没有用到这个。
    
	new HandshakeResponseMessage(
			userName, charset, scramble, "mysql_native_password", db
	)
  }
}
