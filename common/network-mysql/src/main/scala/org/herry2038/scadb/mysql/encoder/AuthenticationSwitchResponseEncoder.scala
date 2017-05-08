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

import org.herry2038.scadb.mysql.message.client.{AuthenticationSwitchResponse, ClientMessage}
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.exceptions.UnsupportedAuthenticationMethodException
import org.herry2038.scadb.mysql.encoder.auth.AuthenticationMethod
import java.nio.charset.Charset
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper

class AuthenticationSwitchResponseEncoder( charset : Charset ) extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {
    val switch = message.asInstanceOf[AuthenticationSwitchResponse]

    val method = switch.request.method
    val authenticator = AuthenticationMethod.Availables.getOrElse(
    method, { throw new UnsupportedAuthenticationMethodException(method) })

    val buffer = ByteBufferUtils.packetBuffer()

    val bytes = authenticator.generateAuthentication(charset, switch.password, switch.request.seed.getBytes(charset) )
    buffer.writeBytes(bytes)

    buffer
  }
  
  
    def decode(buffer: ByteBuf): ClientMessage = {
	    new AuthenticationSwitchResponse(
	      Some(buffer.readCString(charset)),null
	    )
  	}

}
