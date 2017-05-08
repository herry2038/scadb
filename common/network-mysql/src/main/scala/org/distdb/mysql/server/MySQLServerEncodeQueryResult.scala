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

object MySQLServerEncodeQueryResult {
  val log = Log.get[MySQLServerEncodeQueryResult]
}


class MySQLServerEncodeQueryResult(charset: Charset) extends MessageToMessageEncoder[ResultSet] {
	def encode(ctx: ChannelHandlerContext, r: ResultSet, out: java.util.List[Object]): Unit = {
	    val qrm = new QueryResultMessage(r.columnNames.size)
        out.add(qrm)
        
        val registry = new DecoderRegistry(CharsetUtil.UTF_8)
    
        r.columnNames.foreach{c =>
        	val msg = new ColumnDefinitionMessage(
        	        "", // catalog
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
        
        val eofMessage = new EOFMessage(0,0)
        out.add(eofMessage)
	}
}