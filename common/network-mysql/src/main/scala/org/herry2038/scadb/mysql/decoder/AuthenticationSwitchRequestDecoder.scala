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
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.mysql.message.server.{AuthenticationSwitchRequest, ServerMessage}
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.mysql.message.server.ServerMessage

class AuthenticationSwitchRequestDecoder( charset : Charset ) extends MessageDecoder {
  def decode(buffer: ByteBuf): ServerMessage = {
    new AuthenticationSwitchRequest(
      buffer.readCString(charset),
      buffer.readUntilEOF(charset)
    )
  }
  
  def encode(message : ServerMessage): ByteBuf = {
    val switch = message.asInstanceOf[AuthenticationSwitchRequest]
    val buffer = ByteBufferUtils.packetBuffer()
    
    buffer.writeBytes(switch.method.getBytes)
    buffer.writeByte(0)
    
    buffer.writeBytes(switch.seed.getBytes)
    
    buffer
  }
}
