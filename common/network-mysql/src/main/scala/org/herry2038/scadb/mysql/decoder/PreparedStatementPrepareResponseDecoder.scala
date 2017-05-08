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

import org.herry2038.scadb.mysql.message.server.{PreparedStatementPrepareResponse, ServerMessage}
import org.herry2038.scadb.db.util.BufferDumper
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.mysql.message.server.ServerMessage
import org.herry2038.scadb.util.Log

class PreparedStatementPrepareResponseDecoder extends MessageDecoder {

  final val log = Log.get[PreparedStatementPrepareResponseDecoder]

  def decode(buffer: ByteBuf): ServerMessage = {

    //val dump = MySQLHelper.dumpAsHex(buffer)
    //log.debug("prepared statement response dump is \n{}", dump)

    val statementId = Array[Byte]( buffer.readByte(), buffer.readByte(), buffer.readByte(), buffer.readByte() )
    val columnsCount = buffer.readUnsignedShort()
    val paramsCount = buffer.readUnsignedShort()

    // filler
    buffer.readByte()

    val warningCount = buffer.readShort()

    new PreparedStatementPrepareResponse(
      statementId = statementId,
      warningCount = warningCount,
      columnsCount = columnsCount,
      paramsCount = paramsCount
    )
  }
  
  
  def encode(message: ServerMessage): ByteBuf = {
    val psp = message.asInstanceOf[PreparedStatementPrepareResponse]
    val buffer = ByteBufferUtils.packetBuffer()
    buffer.writeBytes(psp.statementId)
    buffer.writeShort(psp.columnsCount)
    buffer.writeShort(psp.paramsCount)
    buffer.writeByte(0) // filler
    
    buffer
  }

}
