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
import org.herry2038.scadb.mysql.message.server.ColumnDefinitionMessage
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import java.nio.charset.Charset
import org.herry2038.scadb.mysql.codec.DecoderRegistry
import org.herry2038.scadb.mysql.encoder.MessageEncoder
import org.herry2038.scadb.mysql.message.server.ServerMessage
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.db.column.StringEncoderDecoder
import org.herry2038.scadb.util.Log

object ColumnDefinitionDecoder {
  final val log = Log.get[ColumnDefinitionDecoder]
}

class ColumnDefinitionDecoder(charset: Charset, registry : DecoderRegistry, returnStr: Boolean = false) extends MessageDecoder {

  override def decode(buffer: ByteBuf): ColumnDefinitionMessage = {

    val catalog = buffer.readLengthEncodedString(charset)
    val schema = buffer.readLengthEncodedString(charset)
    val table = buffer.readLengthEncodedString(charset)
    val originalTable = buffer.readLengthEncodedString(charset)
    val name = buffer.readLengthEncodedString(charset)
    val originalName = buffer.readLengthEncodedString(charset)

    buffer.readBinaryLength

    val characterSet = buffer.readUnsignedShort()
    val columnLength = buffer.readUnsignedInt()
    val columnType = buffer.readUnsignedByte()
    val flags = buffer.readShort()
    val decimals = buffer.readByte()

    buffer.readShort()

    new ColumnDefinitionMessage(
      catalog,
      schema,
      table,
      originalTable,
      name,
      originalName,
      characterSet,
      columnLength,
      columnType,
      flags,
      decimals,
      registry.binaryDecoderFor(columnType, characterSet),
      if ( returnStr ) StringEncoderDecoder else registry.textDecoderFor(columnType,characterSet)
    )
  }
  
  override def encode(message: ServerMessage): ByteBuf = {
    val coldef = message.asInstanceOf[ColumnDefinitionMessage]
    val buffer = ByteBufferUtils.packetBuffer()
    
    buffer.writeLenghtEncodedString(coldef.catalog, charset)
    buffer.writeLenghtEncodedString(coldef.schema, charset)
    buffer.writeLenghtEncodedString(coldef.table, charset)
    buffer.writeLenghtEncodedString(coldef.originalTable, charset)
    buffer.writeLenghtEncodedString(coldef.name, charset)
    buffer.writeLenghtEncodedString(coldef.originalName, charset)
    
    buffer.writeByte(12) // filter_1 , remain length
    
    buffer.writeShort(coldef.characterSet)
    buffer.writeInt(coldef.columnLength.toInt)
    buffer.writeByte(coldef.columnType)
    buffer.writeShort(coldef.flags)
    buffer.writeByte(coldef.decimals)
    buffer.writeShort(0) // filter_2 
    
    buffer
  }

}