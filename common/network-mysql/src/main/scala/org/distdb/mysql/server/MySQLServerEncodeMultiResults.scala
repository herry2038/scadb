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

object MySQLServerEncodeMultiResults {
  val log = Log.get[MySQLServerEncodeMultiResults]
}


class MySQLServerEncodeMultiResults(charset: Charset) extends MessageToMessageEncoder[MultiResults] {
  def encode(ctx: ChannelHandlerContext, rs: MultiResults, out: java.util.List[Object]): Unit = {
    rs.resultCompactor.compact(rs.results, out)
  }
}

