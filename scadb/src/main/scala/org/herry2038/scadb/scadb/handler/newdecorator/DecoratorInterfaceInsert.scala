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

import com.alibaba.druid.sql.ast.{SQLStatement}
import com.alibaba.druid.sql.ast.expr.{SQLIntegerExpr, SQLVariantRefExpr, SQLValuableExpr, SQLIdentifierExpr}
import com.alibaba.druid.sql.ast.statement.{SQLInsertStatement}

import org.herry2038.scadb.mysql.codec.{MySQLHandlerDelegate, MessageTrack}
import org.herry2038.scadb.mysql.message.server.{EOFMessage, OkMessage, ColumnDefinitionMessage}
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatementSingle, ScadbStatement}
import org.herry2038.scadb.scadb.server.processor.{ScadbUuid, Statistics}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * Created by Administrator on 2016/3/16.
 */
class DecoratorInterfaceInsert(statement: SQLStatement, startTime: Long) extends DecoratorInterface(statement, startTime) {
  val readable = false
  val stmt = statement.asInstanceOf[SQLInsertStatement]
  val tableName = stmt.getTableName.getSimpleName

  override val requestType = Statistics.INSERT_TYPE

  override def decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    if ( ! getTable(tableName) ) {
      return null
    }
    var mt: MessageTrack = null

    // 唯一id 处理
    if ( table.uniqIdCol != null ) {
      if ( stmt.getColumns.find{
        case expr: SQLIdentifierExpr => expr.getSimpleName == table.uniqIdCol
      } == None ) {
          handler.lastInsertId = ScadbUuid.uuid
          stmt.addColumn(new SQLIdentifierExpr(table.uniqIdCol))
          stmt.getValues.getValues.add(new SQLIntegerExpr(handler.lastInsertId))
          mt = new UuidMessageTracker(handler.lastInsertId)
      }
    }

    if ( table.ruleName == null )
      return super.decorator(handler, mt)

    //stmt.getTableName().getSimpleName();

    if ( stmt.getColumns().size() != stmt.getValues().getValues.size() ) {
      errmsg = "syntax error!"
      return null
    }
    for ( i <- 0 until stmt.getColumns().size() ) {
      if ( !stmt.getColumns().get(i).isInstanceOf[SQLIdentifierExpr] ) {
        errmsg = "syntax error, insert item must be field name!"
        return null
      }

      if ( stmt.getColumns().get(i).asInstanceOf[SQLIdentifierExpr].getLowerName() == table.column ) {
        if ( !stmt.getValues().getValues().get(i).isInstanceOf[SQLValuableExpr] ) {
          errmsg = "syntax error, partition key 's value must be valuable!"
          return null
        }
        val result = ScadbConfig.conf.getPartitionAndServer(rule, stmt.getValues().getValues().get(i).asInstanceOf[SQLValuableExpr].getValue().toString(), table.mysqls)

        setPartition(result._1, result._3)
        return new ScadbStatementSingle(handler, statement.toString(), result._2, startTime, requestType, false, mt)
      }
    }
    null
  }

  override def setPartition(partition: String, prefix: String): Unit = {
    stmt.getTableSource().getExpr.asInstanceOf[SQLIdentifierExpr].
      setName(if( prefix != null ) prefix + "." + tableName + "_" + partition else tableName + "_" + partition)
  }
}

class UuidMessageTracker(val uuid: Long) extends MessageTrack {
  override def onRow(rows: Array[Any]): Unit = None
  override def onEof(delegate: MySQLHandlerDelegate, message: EOFMessage): Unit = None

  override def onOk(delegate: MySQLHandlerDelegate, msg: OkMessage): Unit =
    delegate.onOk(new OkMessage(msg.affectedRows, uuid, msg.statusFlags, msg.warnings, msg.message))
  override def close(): Unit = None
  override def onColumns(columns: ArrayBuffer[ColumnDefinitionMessage]): Unit = None
}
