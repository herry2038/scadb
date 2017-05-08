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
import org.herry2038.scadb.mysql.message.server.OkMessage
import org.herry2038.scadb.util.Log
import scala.collection.mutable.ArrayBuffer

object MySQLServerEncodeMultiResults {
  val log = Log.get[MySQLServerEncodeMultiResults]
}


class MySQLServerEncodeMultiResults(charset: Charset) extends MessageToMessageEncoder[MultiResults] {
  def encode(ctx: ChannelHandlerContext, rs: MultiResults, out: java.util.List[Object]): Unit = {
    rs.resultCompactor.compact(rs.results, out)
  }
}

