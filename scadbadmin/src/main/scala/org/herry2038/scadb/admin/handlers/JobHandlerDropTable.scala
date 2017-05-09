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
package org.herry2038.scadb.admin.handlers

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement
import com.google.gson.Gson
import org.herry2038.scadb.admin.JobHandler
import org.herry2038.scadb.admin.model.BusinessWrapper
import org.herry2038.scadb.conf.ScadbConf

import scala.collection.mutable

class JobHandlerDropTable(business: String, statement: SQLStatement, hints: mutable.HashMap[String,String]) extends JobHandler(business, statement, hints) {
  val stmt = statement.asInstanceOf[SQLDropTableStatement]
  override def tableName: String = stmt.getTableSources.get(0).getExpr.asInstanceOf[SQLIdentifierExpr].getName

  override def setTableName(tableName: String): Unit = stmt.getTableSources.get(0).getExpr.asInstanceOf[SQLIdentifierExpr].setName(tableName)

  override def handle(busi: BusinessWrapper) : Unit = {
    val t = tableName
    super.handle(busi)
    ScadbConf.client.delete.forPath(s"${ScadbConf.basePath}/businesses/$business/tables/" + t)
  }
}
