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

import java.util

import org.herry2038.scadb.db.QueryResult
import org.herry2038.scadb.mysql.codec.DecoderRegistry
import org.herry2038.scadb.mysql.column.ColumnTypes
import org.herry2038.scadb.mysql.message.server._
import org.herry2038.scadb.db.util.ByteBufferUtils
import io.netty.util.CharsetUtil

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/8/26.
 */
class ResultCompactorCommon extends ResultCompactor {
  override def compact(rs: ArrayBuffer[QueryResult], out: util.List[Object]): Unit = {
    var isResultSet = false
    var affectedRows : Long = 0
    rs.foreach { r1=>
      affectedRows += r1.rowsAffected
      r1.rows.map { r =>
        if ( ! isResultSet ) {
          isResultSet = true
          val qrm = new QueryResultMessage(r.columnNames.size)
          out.add(qrm)

          val registry = new DecoderRegistry(CharsetUtil.UTF_8)

          r.columnNames.foreach{c =>
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
        }
        r.foreach{row =>
          val resultSetRow = new ResultSetRowMessage
          row.foreach{f =>
            val buf = if ( f == null ) null else {
              val b = ByteBufferUtils.mysqlBuffer(1024)
              b.writeBytes(f.asInstanceOf[String].getBytes(CharsetUtil.UTF_8))
            }
            resultSetRow += buf
          }
          out.add(resultSetRow)
        }
      }
    }
    if ( isResultSet ) {
      val eofMessage = new EOFMessage(0,0)
      out.add(eofMessage)
    } else {
      out.add(new OkMessage(affectedRows,0,0,0,null))
    }
  }
}

object ResultCompactorCommon {
  val resultCompactorCommon = new ResultCompactorCommon()
}
