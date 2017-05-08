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
package org.herry2038.scadb.scadb.handler.newdecorator

import java.io.{IOException, FileOutputStream, File}

import com.alibaba.druid.sql.ast.{SQLExpr, SQLStatement}
import com.alibaba.druid.sql.ast.expr._
import com.alibaba.druid.sql.ast.statement.{SQLExprTableSource, SQLSelectStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock
import org.herry2038.scadb.mysql.codec.{MySQLHandlerDelegate, MessageTrack}
import org.herry2038.scadb.mysql.message.server.{EOFMessage, ErrorMessage, OkMessage, ColumnDefinitionMessage}
import org.apache.commons.io.{IOUtils, FileUtils}
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatement, ScadbStatementSingle, ScadbStatementParallelMulti}
import org.herry2038.scadb.util.Log
import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by Administrator on 2016/3/15.
 */
class DecoratorInterfaceSelectInto(statement: SQLStatement, startTime: Long) extends DecoratorInterface(statement, startTime) {
  val log = Log.get[DecoratorInterfaceSelectInto]
  val readable = true
  val stmt = statement.asInstanceOf[SQLSelectStatement]
  val tableName = Option(stmt.getSelect().getQuery().
    asInstanceOf[MySqlSelectQueryBlock].getFrom().
    asInstanceOf[SQLExprTableSource]).map(_.getExpr.asInstanceOf[SQLIdentifierExpr].getName).getOrElse(null)
  override def setPartition(partition: String, prefix: String): Unit = ???

  private var beforeTableSql: String = null
  private var whereSql : String = null
  private var localDir : String = null
  private def intoClauseToLocalSql(into: MySqlOutFileExpr, buf: StringBuffer): Unit = {
    buf.append(" INTO LOCALFILE 'a' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'")

//    if ( into.getColumnsTerminatedBy != null )
//      buf.append(" FIELDS TERMINATED BY ").append(into.getColumnsTerminatedBy.toString)
//    if ( into.getColumnsEnclosedBy != null )
//      buf.append("OPTIONALLY ENCLOSED BY ").append(into.getColumnsEnclosedBy.toString)
//    if ( into.getLinesTerminatedBy != null )
//      buf.append("LINES TERMINATED BY ").append(into.getLinesTerminatedBy.toString)

    localDir = {
      val f = into.getFile.toString
      f.substring(1, f.length-1)
    }
    val f = new File(localDir)
    if ( ! f.exists() ) {
      FileUtils.forceMkdir(f)
    }
    if ( !f.exists || ! f.isDirectory ) {
      throw new RuntimeException(s"directory  ${localDir} not exists!")
    }
  }


  override def decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    if ( tableName == null || tableName.charAt(0) == '$' || ! getTable(tableName) ) {
      errno = -1
      errmsg = "unknown table!!!"
      return null
    }


    val buf = new StringBuffer("SELECT ")
    val queryBlock = stmt.getSelect.getQuery.asInstanceOf[MySqlSelectQueryBlock]
    queryBlock.getSelectList.foreach(item=>buf.append(item.toString).append(","))

    buf.delete(buf.length() -1 , buf.length)
    intoClauseToLocalSql(queryBlock.getInto.getExpr.asInstanceOf[MySqlOutFileExpr], buf)
    buf.append(" FROM ")

    beforeTableSql = buf.toString
    whereSql = Option(queryBlock.getWhere).map(" WHERE " + _.toString).getOrElse("")

    if ( table.ruleName == null ) {
      new ScadbStatementSingle(handler, beforeTableSql + tableName + whereSql, ScadbConfig.conf.busi.default, startTime, requestType, handler.isReadWriteSplit)
    }

    Option(decoratorWhere(handler, stmt.getSelect().getQuery().asInstanceOf[MySqlSelectQueryBlock].getWhere)).getOrElse {
      val sqls = new java.util.ArrayList[(String, String, MessageTrack)]
      try {
        rule.partitions(tableName, table.mysqls, ScadbConfig.conf.busi.dbPrefix, ScadbConfig.conf.busi.dbNums).foreach { i =>
          sqls.add((i._3, getSql(i._1, ScadbConfig.conf.busi.dbPrefix), createMessageTrack(i._1)))
        }
      } catch {
        case ex: IOException =>
          sqls.foreach(_._3.close)
          throw ex
      }
      new  ScadbStatementParallelMulti(handler,sqls, ScadbConfig.conf.parallelDegree,
        startTime, requestType,
        readable && handler.isReadWriteSplit)
    }
  }

  private def getSql(partition: String, prefix: String): String = {
    val name = if( prefix != null ) prefix + (partition.toInt % ScadbConfig.conf.busi.dbNums) + "." + tableName + "_" + partition else tableName + "_" + partition
    beforeTableSql + name + whereSql
  }

  private def createMessageTrack(partition: String): MessageTrack = {
    new MessageTrack {
      private var fileName = localDir + "/" + (if ( partition == null ) tableName else tableName + "_" + partition)
      private var outputStream = {
        val file = new File(fileName)
        FileUtils.openOutputStream(file)
      }
      private var rows: Long = 0
      private var error: String = null

      override def onRow(row: Array[Any]): Unit = {
        if (error == null) {
          val num = row(0).asInstanceOf[String].toInt
          rows += num
          val data = row(1).asInstanceOf[String]
          try {
            IOUtils.write(data, outputStream)
          } catch {
            case ex: Throwable =>
              log.error(s"werite file ${fileName} error!", ex)
              error = s"open file ${fileName} error: " + ex.getMessage
          }
        }
      }

      override def onEof(delegate: MySQLHandlerDelegate, message : EOFMessage): Unit = {
        try { outputStream.close } finally {}
        if ( error == null)
            delegate.onOk(new OkMessage(rows, 0, 0, 0, null))
        else
            delegate.onError(new ErrorMessage(-1, error))
      }

      override def onOk(delegate: MySQLHandlerDelegate, msg: OkMessage): Unit = {}

      override def onColumns(columns: ArrayBuffer[ColumnDefinitionMessage]): Unit = {}

      override def close(): Unit = {
        if ( outputStream != null )
          try { outputStream.close } finally {}
      }
    }
  }

  override def decoratorWhere(handler: MySQLServerConnectionHandler, where: SQLExpr): ScadbStatement = where match {
    case binaryCondi: SQLBinaryOpExpr =>
      if (binaryCondi.getOperator == SQLBinaryOperator.Equality && binaryCondi.getLeft().isInstanceOf[SQLIdentifierExpr] && binaryCondi.getLeft().asInstanceOf[SQLIdentifierExpr].getName() == table.column ) {
        if ( binaryCondi.getRight().isInstanceOf[SQLValuableExpr] ) {
          val result = ScadbConfig.conf.getPartitionAndServer(rule, binaryCondi.getRight().asInstanceOf[SQLValuableExpr].getValue().toString(), table.mysqls)
          val sql = getSql(result._1, result._3)
          log.info(s"execute sql:{} on server ${result._2}", sql)
          val mt = createMessageTrack(result._1) // if failed will throw
          new ScadbStatementSingle(handler, sql, result._2, startTime, requestType, readable && handler.isReadWriteSplit, mt)
        } else null
      } else if ( binaryCondi.getOperator == SQLBinaryOperator.BooleanAnd ) {
        decoratorWhere(handler, binaryCondi.getLeft) match {
          case null => decoratorWhere(handler, binaryCondi.getRight)
          case statement: ScadbStatement => statement
        }
      } else
        null
    case inCondi: SQLInListExpr =>
      if ( inCondi.getExpr.isInstanceOf[SQLIdentifierExpr] && inCondi.getExpr.asInstanceOf[SQLIdentifierExpr].getName() == table.column ) {
        val finalPartitions = inCondi.getTargetList.foldLeft(new HashMap[String,(String,String,java.util.List[SQLExpr])]) { // ( MySQL服务,  数据库前缀, 同一个分区中的分区键 )
          (partitions,currentItem) =>
            if ( !currentItem.isInstanceOf[SQLValuableExpr]) return null
            val (partition,server, prefix) = ScadbConfig.conf.getPartitionAndServer(rule, currentItem.asInstanceOf[SQLValuableExpr].getValue.toString, table.mysqls)
            val obj = partitions.get(partition) match {
              case Some((server, prefix, items)) =>
                items.add(currentItem)
              case None =>
                val items = new java.util.ArrayList[SQLExpr]
                items.add(currentItem)
                partitions += (partition -> (server, prefix, items))
            }
            partitions
        }
        val sqls = new java.util.ArrayList[(String,String,MessageTrack)]

        try {
          finalPartitions.foreach {
            case (k: String, v) => // v 的类型：(String,String,java.util.List[HiveItem])
              sqls.add((v._1, getSql(k, v._2), createMessageTrack(k)))
          }
          return new ScadbStatementParallelMulti(handler,sqls, ScadbConfig.conf.parallelDegree,
            startTime, requestType,
            handler.isReadWriteSplit)
        } catch {
          case ex: Throwable =>
            sqls.foreach(_._3.close)
            errmsg = ex.getMessage
            throw new RuntimeException(errmsg)
        }
      } else
        null
    case _ =>
      null
  }
}
