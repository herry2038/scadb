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
import com.alibaba.druid.sql.ast.expr._
import com.alibaba.druid.sql.ast.statement.{SQLExprTableSource, SQLSelectStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock
import org.herry2038.scadb.db.pool.{AsyncObjectPool}
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbMySQLPools
import org.herry2038.scadb.scadb.handler.executeplan.ExecutePlan
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatementSystem, ScadbStatement}
import org.herry2038.scadb.scadb.server.processor.Statistics
import scala.collection.JavaConversions._
/**
 * Created by Administrator on 2016/3/15.
 */
class DecoratorInterfaceQuery(statement: SQLStatement, startTime: Long) extends DecoratorInterface(statement, startTime) {
  val readable = true
  override val requestType = Statistics.SELECT_TYPE

  val stmt = statement.asInstanceOf[SQLSelectStatement]
  val tableName = Option(stmt.getSelect().getQuery().
    asInstanceOf[MySqlSelectQueryBlock].getFrom().
    asInstanceOf[SQLExprTableSource]).map(_.getExpr.asInstanceOf[SQLIdentifierExpr].getName).getOrElse(null)

  private def decoratorSys(handler: MySQLServerConnectionHandler): ScadbStatement = {
    val metaBuf = new StringBuffer()
    val dataBuf = new StringBuffer()
    if ( tableName == null ) {
      try {
        statement.asInstanceOf[SQLSelectStatement].getSelect().getQuery().
          asInstanceOf[MySqlSelectQueryBlock].getSelectList.map {
          selectItem =>
            metaBuf.append(Option(selectItem.getAlias).getOrElse(selectItem.getExpr.toString())).append('|')
            dataBuf.append(getValue(selectItem.getExpr(), handler)).append('|')
        }
      } catch {
        case ex: Exception =>
          errmsg = ex.getMessage
          return null
      }
      metaBuf.delete(metaBuf.length() - 1, metaBuf.length)
      dataBuf.delete(dataBuf.length() - 1, dataBuf.length)
    } else {
      tableName match {
        case "$pools" =>
          metaBuf.append("proxy|pool|status")
          ScadbMySQLPools.instances.map { case (proxy, proxyInfo)=>
            proxyInfo.pools.map { case (pool, poolInfo) =>
              val status = poolInfo.asInstanceOf[AsyncObjectPool[Any]].status
              dataBuf.append(proxy).append("(").append(proxyInfo.master).append(',').append(proxyInfo.thisGroups).
                append(",").append(proxyInfo.currentSeeds).
                append(")|").
              append(pool).append("|").append(status).append('\n')
            }
          }
          if ( dataBuf.length() > 0 ) {
            dataBuf.delete(dataBuf.length() - 1, dataBuf.length)
          }
        case _ =>
          errmsg = "unknown system table name"
          return null
      }
    }
    return new ScadbStatementSystem(handler, metaBuf.toString(), dataBuf.toString())
  }

  override def decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    if ( tableName == null || tableName.charAt(0) == '$' ) {
      return decoratorSys(handler)
    }

    if ( ! getTable(tableName) ) {
      return null
    }

    if ( table.ruleName == null )
      return super.decorator(handler)

    Option(decoratorWhere(handler, stmt.getSelect().getQuery().
      asInstanceOf[MySqlSelectQueryBlock].getWhere)).getOrElse {
      val plan = new ExecutePlan(stmt, tableName, startTime, requestType, rule, handler)
      plan.createPlan()
    }
  }

  override def setPartition(partition: String, prefix: String): Unit = {
    statement.asInstanceOf[SQLSelectStatement].getSelect().getQuery().
      asInstanceOf[MySqlSelectQueryBlock].getFrom().
      asInstanceOf[SQLExprTableSource].getExpr.asInstanceOf[SQLIdentifierExpr].
      setName(if( prefix != null ) prefix + "." + tableName + "_" + partition else tableName + "_" + partition)
  }
}
