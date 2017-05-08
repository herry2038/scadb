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
package org.herry2038.scadb.mysql.server

import io.netty.handler.codec.MessageToMessageEncoder
import java.nio.charset.Charset
import org.herry2038.scadb.db.QueryResult
import io.netty.channel.ChannelHandlerContext
import org.herry2038.scadb.db.ResultSet
import org.herry2038.scadb.mysql.message.server.QueryResultMessage
import org.herry2038.scadb.mysql.codec.DecoderRegistry
import org.herry2038.scadb.mysql.message.server.ColumnDefinitionMessage
import org.herry2038.scadb.mysql.column.ColumnTypes
import org.herry2038.scadb.mysql.message.server.ColumnProcessingFinishedMessage
import org.herry2038.scadb.mysql.message.server.ResultSetRowMessage
import org.herry2038.scadb.db.util.ByteBufferUtils
import org.herry2038.scadb.db.util.ChannelWrapper.bufferToWrapper
import io.netty.util.CharsetUtil
import org.herry2038.scadb.mysql.message.server.EOFMessage
import org.herry2038.scadb.util.Log

object MySQLServerEncodeSysResult {
  val log = Log.get[MySQLServerEncodeSysResult]
}


class MySQLServerEncodeSysResult(charset: Charset) extends MessageToMessageEncoder[(String,String)] {
	def encode(ctx: ChannelHandlerContext, r: (String,String), out: java.util.List[Object]): Unit = {
	    val columns = r._1.split("\\|")
        val qrm = new QueryResultMessage(columns.size.toInt)
        out.add(qrm)
        val registry = new DecoderRegistry(CharsetUtil.UTF_8)
        
        columns.foreach{c =>
            val msg = new ColumnDefinitionMessage(
                    "def", // catalog
                    "", // schema
                    "", // table
                    "", // originalTable
                    c,
                    c,
                    83,
                    255,
                    ColumnTypes.FIELD_TYPE_VARCHAR,
                    0,
                    0,                     
                  registry.binaryDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR, 83),
                  registry.textDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR,83) )
            
            out.add(msg)
        }
            
        val fieldEndMsg = new ColumnProcessingFinishedMessage(new EOFMessage(0,0))
        out.add(fieldEndMsg)
        if ( ! r._2.isEmpty ) {
          val rows = r._2.split("\n")
          rows.foreach { row =>
            val cols = row.split("\\|")

            val resultSetRow = new ResultSetRowMessage
            cols.foreach { f =>
              val buf = ByteBufferUtils.mysqlBuffer(1024)
              buf.writeBytes(f.getBytes(CharsetUtil.UTF_8))
              resultSetRow += buf
            }
            out.add(resultSetRow)
          }
        }
            
        val eofMessage = new EOFMessage(0,0)
        out.add(eofMessage)
	}
}