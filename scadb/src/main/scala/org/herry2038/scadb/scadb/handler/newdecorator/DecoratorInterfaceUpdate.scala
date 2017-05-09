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
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.{SQLExprTableSource, SQLUpdateStatement}
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatement, ScadbStatement$}
import org.herry2038.scadb.scadb.server.processor.Statistics


class DecoratorInterfaceUpdate(statement: SQLStatement, startTime: Long) extends DecoratorInterface(statement, startTime) {
  val readable = false
  val stmt = statement.asInstanceOf[SQLUpdateStatement]
  val tableName = stmt.getTableName.getSimpleName()
  override val requestType = Statistics.UPDATE_TYPE

  override def decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    if ( ! getTable(tableName)) {
      return null
    }

    if ( table.ruleName == null )
      return super.decorator(handler)

    decoratorWhere(handler, stmt.getWhere)

  }

  override def setPartition(partition: String, prefix: String): Unit = {
    stmt.getTableSource().asInstanceOf[SQLExprTableSource].getExpr().asInstanceOf[SQLIdentifierExpr].
      setName(if( prefix != null ) prefix + "." + tableName + "_" + partition else tableName + "_" + partition)
  }


  
}
