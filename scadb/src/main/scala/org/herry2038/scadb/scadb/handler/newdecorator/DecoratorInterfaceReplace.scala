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

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.expr.{SQLIdentifierExpr, SQLValuableExpr}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatement, ScadbStatementSingle}
import org.herry2038.scadb.scadb.server.processor.Statistics

/**
 * Created by Administrator on 2016/3/16.
 */
class DecoratorInterfaceReplace(statement: SQLStatement, startTime: Long) extends DecoratorInterface(statement, startTime) {
  val readable = false
  val stmt = statement.asInstanceOf[MySqlReplaceStatement]
  val tableName = stmt.getTableName.getSimpleName

  override val requestType = Statistics.REPLACE_TYPE


  override def decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    if ( ! getTable(tableName) ) {
      return null
    }

    if ( table.ruleName == null )
      return super.decorator(handler)


    //stmt.getTableName().getSimpleName();
    if ( stmt.getValuesList.size() > 1 ) {
      errmsg = "cannot support multi values!"
      return null
    }
    if ( stmt.getColumns().size() != stmt.getValuesList.get(0).getValues().size() ) {
      errmsg = "syntax error!"
      return null
    }
    for ( i <- 0 until stmt.getColumns().size() ) {
      if ( !stmt.getColumns().get(i).isInstanceOf[SQLIdentifierExpr] ) {
        errmsg = "syntax error, insert item must be field name!"
        return null
      }

      if ( stmt.getColumns().get(i).asInstanceOf[SQLIdentifierExpr].getLowerName() == table.column ) {
        if ( !stmt.getValuesList.get(0).getValues().get(i).isInstanceOf[SQLValuableExpr] ) {
          errmsg = "syntax error, partition key 's value must be valuable!"
          return null
        }
        val result = ScadbConfig.conf.getPartitionAndServer(rule, stmt.getValuesList.get(0).getValues().get(i).asInstanceOf[SQLValuableExpr].getValue().toString(), table.mysqls)

        setPartition(result._1, result._3)
        return new ScadbStatementSingle(handler, statement.toString(), result._2, startTime, requestType, false)
      }
    }
    null
  }

  override def setPartition(partition: String, prefix: String): Unit = {
    stmt.getTableSource().getExpr.asInstanceOf[SQLIdentifierExpr].
      setName(if( prefix != null ) prefix + "." + tableName + "_" + partition else tableName + "_" + partition)
  }
}
