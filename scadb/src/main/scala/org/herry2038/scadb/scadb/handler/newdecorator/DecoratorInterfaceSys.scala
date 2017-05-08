package org.herry2038.scadb.scadb.handler.newdecorator

import java.sql.SQLException

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.statement.{SQLExplainStatement, SQLRollbackStatement, SQLShowTablesStatement, SQLSetStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.{MySqlCommitStatement, MySqlShowCreateTableStatement, MySqlShowDatabasesStatement, MySqlSetNamesStatement}
import org.herry2038.scadb.db.general.{MutableResultSet}
import org.herry2038.scadb.mysql.codec.{MySQLHandlerDelegate, MessageTrack, DecoderRegistry}
import org.herry2038.scadb.mysql.column.ColumnTypes
import org.herry2038.scadb.mysql.message.server._
import org.herry2038.scadb.db.util.ByteBufferUtils
import io.netty.util.CharsetUtil
import org.apache.commons.lang3.StringUtils
import org.herry2038.scadb.mysql.server.{MySQLServerHandlerDelegate, MySQLServerConnectionHandler}
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatementSystem, ScadbStatement, ScadbStatementMulti, ScadbStatementSingle}
import org.herry2038.scadb.scadb.server.processor.{ScadbProcessor}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * Created by Administrator on 2016/3/22.
 */
class DecoratorInterfaceSys(sql: String,statement: SQLStatement) extends DecoratorInterface(statement, 0) {
  override def readable(): Boolean = ???

  override def tableName(): String = ???

  override def setPartition(partition: String, prefix: String): Unit = ???

  def doWithSet(stmt: SQLSetStatement) :Unit = {
    stmt.getItems().foreach { assign =>
      val name = assign.getTarget.toString()
      var value = assign.getValue().toString()
      if ( value.charAt(0) == '\'' )
        value = value.substring(1, value.length() -1)
      name match {
        case "loggingSql" =>
          var loggingFlags = 0
          value.split(",").foreach { t =>
            t match {
              case "query" => loggingFlags = loggingFlags | ( 1 << 0 )
              case "dml"   => loggingFlags = loggingFlags | ( 1 << 1 )
              case "ddl"   => loggingFlags = loggingFlags | ( 1 << 2 )
              case "all"   => loggingFlags = loggingFlags | 7
              case "clear" => loggingFlags = 0
            }
          }
          ScadbConfig.conf.loggingSql = loggingFlags
        case _ =>
      }
    }
  }

  override def decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    if ( statement.isInstanceOf[MySqlSetNamesStatement] ) {
      // just return ok ,we will only support one character, that will be configed in the xml, either utf8 or utf8mb4
      handler.write(new OkMessage(0,0,0,0,""))
      return ScadbStatement.ScadbStatementNull
    } else if ( statement.isInstanceOf[MySqlShowDatabasesStatement] ) {
      handler.write(("Database", "scadb"))
      return ScadbStatement.ScadbStatementNull
    } else if ( statement.isInstanceOf[SQLShowTablesStatement] ) {
      handler.write(("Tables_in_scadb", StringUtils.join(ScadbConfig.conf.tables.keySet(), "\n")))
      return ScadbStatement.ScadbStatementNull
    } else if ( statement.isInstanceOf[MySqlCommitStatement] || statement.isInstanceOf[SQLRollbackStatement]) {
      handler.write(MySQLServerHandlerDelegate.okMessage)
      return ScadbStatement.ScadbStatementNull
    } else if ( statement.isInstanceOf[SQLSetStatement]) {

      doWithSet(statement.asInstanceOf[SQLSetStatement])
      handler.write(new OkMessage(0,0,0,0,""))
      return ScadbStatement.ScadbStatementNull
    } else if ( statement.isInstanceOf[MySqlShowCreateTableStatement]) {
      val name = statement.asInstanceOf[MySqlShowCreateTableStatement].getName.toString()
      val table = ScadbConfig.conf.tables.get(name)
      val (prefixString, rule) = if ( table.ruleName == null ) ("", null) else (s"/*!scadb:partitionkey=${table.column} rule=${table.ruleName}*/" , ScadbConfig.conf.partitionRules.get(table.ruleName) )
      // (分区号，对应的MySQL, 数据库前缀 )

      if ( table.ruleName != null && rule == null ) {
        errmsg = s"unknown rule name ${table.ruleName} for table ${name}}!"
        return null
      }
      val (showTableName,realTableName,mysql) = if ( rule == null ) (name, name, ScadbConfig.conf.busi.default) else {
        val (partitionNo, mysql, dbPrefix) = ScadbConfig.conf.getPartitionAndServer(rule, "0", ScadbConfig.conf.busi.mysqls)
        ((if (dbPrefix == null) "" else dbPrefix + ".") + name + "_" + partitionNo, name + "_" + partitionNo,  mysql)
      }
      val strSQL = "show create table " + showTableName
      return new ScadbStatementSingle(handler, strSQL, mysql, startTime, requestType,
        false,
        new MessageTrack {
          private var currentQuery: MutableResultSet[ColumnDefinitionMessage] = null
          override def onEof(delegate: MySQLHandlerDelegate, message : EOFMessage): Unit = delegate.onResultSet(currentQuery, message)
          override def onRow(rows: Array[Any]): Unit = {
            rows.update(0, name)
            rows.update(1, prefixString + StringUtils.replaceOnce(rows.apply(1).asInstanceOf[String], realTableName, name))
            currentQuery.addRow(rows)
          }
          override def onOk(delegate: MySQLHandlerDelegate, msg: OkMessage): Unit = {}
          override def close(): Unit = {}
          override def onColumns(columns: ArrayBuffer[ColumnDefinitionMessage]): Unit = currentQuery = new MutableResultSet[ColumnDefinitionMessage](columns)
        })
    } else if ( statement.isInstanceOf[SQLExplainStatement] ) {
      val stmt = statement.asInstanceOf[SQLExplainStatement]
      ScadbProcessor.getDecoratorCommon(sql, stmt.getStatement,startTime).decorator(handler) match {
        case single: ScadbStatementSingle =>
          if ( single.mt != null ) single.mt.close
          return new ScadbStatementSingle(single.handler,"EXPLAIN " + single.sql, single.server,
            startTime, requestType,
            single.isRead, new MessageTrack {
            private var currentQuery: MutableResultSet[ColumnDefinitionMessage] = null
            override def onEof(delegate: MySQLHandlerDelegate, message : EOFMessage): Unit = delegate.onResultSet(currentQuery, message)
            override def onRow(rows: Array[Any]): Unit = {
              rows.update(rows.size-1, rows(rows.size-1) + "\npool:" + single.server + ",rewrite sql:" + StringUtils.replace(single.sql, "\n", " "))
              currentQuery.addRow(rows)
            }
            override def onOk(delegate: MySQLHandlerDelegate, msg: OkMessage): Unit = {}
            override def close(): Unit = {}
            override def onColumns(columns: ArrayBuffer[ColumnDefinitionMessage]): Unit = currentQuery = new MutableResultSet[ColumnDefinitionMessage](columns)
          })
        case multi: ScadbStatementMulti =>
          val buf = new StringBuffer
          multi.sqls.foreach { single =>
            buf.append(single._1).append('|').append(StringUtils.replace(single._2, "\n", " ")).append('\n')
          }
          return new ScadbStatementSystem(handler, "SERVER|SQL", buf.toString)
        case _ =>
          null
      }
    }
    null
  }


}

object DecoratorInterfaceSys {
  val registry = new DecoderRegistry(CharsetUtil.UTF_8)
  def columnToMessage(column: String): ColumnDefinitionMessage = {
    new ColumnDefinitionMessage(
      "def", // catalog
      "", // schema
      "", // table
      "", // originalTable
      column,
      column,
      83,
      255,
      ColumnTypes.FIELD_TYPE_VARCHAR,
      0,
      0,
      registry.binaryDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR, 83),
      registry.textDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR,83) )
  }

  def returnSystemData(handler: MySQLServerConnectionHandler, columns: List[String], values: ArrayBuffer[ArrayBuffer[String]]): ScadbStatement = {
    val out = new java.util.ArrayList[Object]
    out.add(new QueryResultMessage(columns.size))
    columns.foreach(col => out.add(columnToMessage(col)))
    out.add(new ColumnProcessingFinishedMessage(new EOFMessage(0,0)))

    values.foreach { row =>
      val resultSetRow = new ResultSetRowMessage
      row.foreach{f =>
        val buf = ByteBufferUtils.mysqlBuffer(1024)
        if ( f == null )
          buf.writeByte(251)
        else
          buf.writeBytes(f.getBytes(CharsetUtil.UTF_8))
        resultSetRow += buf
      }
      out.add(resultSetRow)
    }
    out.add(new EOFMessage(0,0))
    handler.write(out)
    ScadbStatement.ScadbStatementNull
  }

}
