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