package org.distdb.mysql.server

import java.util

import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.mysql.codec.DecoderRegistry
import com.github.mauricio.async.db.mysql.column.ColumnTypes
import com.github.mauricio.async.db.mysql.message.server._
import com.github.mauricio.async.db.util.ByteBufferUtils
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
