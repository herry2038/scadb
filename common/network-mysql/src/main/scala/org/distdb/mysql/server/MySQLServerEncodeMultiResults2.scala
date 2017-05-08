package org.distdb.mysql.server

import com.github.mauricio.async.db.util.Log
import io.netty.handler.codec.MessageToMessageEncoder
import java.nio.charset.Charset
import com.github.mauricio.async.db.QueryResult
import io.netty.channel.ChannelHandlerContext
import com.github.mauricio.async.db.ResultSet
import com.github.mauricio.async.db.mysql.message.server.QueryResultMessage
import com.github.mauricio.async.db.mysql.codec.DecoderRegistry
import com.github.mauricio.async.db.mysql.message.server.ColumnDefinitionMessage
import com.github.mauricio.async.db.mysql.column.ColumnTypes
import com.github.mauricio.async.db.mysql.message.server.ColumnProcessingFinishedMessage
import com.github.mauricio.async.db.mysql.message.server.ResultSetRowMessage
import com.github.mauricio.async.db.util.ByteBufferUtils
import com.github.mauricio.async.db.util.ChannelWrapper.bufferToWrapper
import io.netty.util.CharsetUtil
import com.github.mauricio.async.db.mysql.message.server.EOFMessage
import com.github.mauricio.async.db.mysql.message.server.OkMessage
import scala.collection.mutable.ArrayBuffer

object MySQLServerEncodeMultiResults2 {
  val log = Log.get[MySQLServerEncodeMultiResults2]
}


class MySQLServerEncodeMultiResults2(charset: Charset) extends MessageToMessageEncoder[ArrayBuffer[QueryResult]] {
  def encode(ctx: ChannelHandlerContext, rs: ArrayBuffer[QueryResult], out: java.util.List[Object]): Unit = {
    var isResultSet = false
    var affectedRows: Long = 0
    rs.foreach { r1 =>
      affectedRows += r1.rowsAffected
      r1.rows.map { r =>
        if (!isResultSet) {
          isResultSet = true
          val qrm = new QueryResultMessage(r.columnNames.size)
          out.add(qrm)

          val registry = new DecoderRegistry(CharsetUtil.UTF_8)

          r.columnNames.foreach { c =>
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
              registry.textDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR, 83))

            out.add(msg)
          }
          val fieldEndMsg = new ColumnProcessingFinishedMessage(new EOFMessage(0, 0))
          out.add(fieldEndMsg)
        }
        r.foreach { row =>
          val resultSetRow = new ResultSetRowMessage
          row.foreach { f =>
            val buf = if (f != null) {
              val b = ByteBufferUtils.mysqlBuffer(1024)
              b.writeBytes(f.asInstanceOf[String].getBytes(CharsetUtil.UTF_8))
            } else null
            resultSetRow += buf
          }
          out.add(resultSetRow)
        }
      }
    }
    if (isResultSet) {
      val eofMessage = new EOFMessage(0, 0)
      out.add(eofMessage)
    } else {
      out.add(new OkMessage(affectedRows, 0, 0, 0, null))
    }
  }
}

