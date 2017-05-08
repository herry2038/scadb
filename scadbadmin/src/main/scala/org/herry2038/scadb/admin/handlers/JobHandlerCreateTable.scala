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
import com.alibaba.druid.sql.ast.statement.{SQLColumnDefinition, SQLCreateTableStatement}
import com.google.gson.Gson
import org.herry2038.scadb.admin.{JobHandler, JobHandlerException}
import org.herry2038.scadb.admin.model.BusinessWrapper
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.ScadbConf.Table
import scala.collection.JavaConversions._
import scala.collection.mutable
import java.util



/**
 * Created by Administrator on 2016/3/10.
 */
class JobHandlerCreateTable(business: String, statement: SQLStatement, hints: mutable.HashMap[String,String]) extends JobHandler(business, statement, hints) {
  val stmt = statement.asInstanceOf[SQLCreateTableStatement]
  val tableName = {
    val name = stmt.getTableSource.getExpr.asInstanceOf[SQLIdentifierExpr].getName()
    if ( name.charAt(0) == '`' )
      name.substring(1, name.length() - 1)
    else
      name
  }


  override def setTableName(tableName: String): Unit = {
    stmt.getTableSource().getExpr.asInstanceOf[SQLIdentifierExpr].
      setName(tableName)
  }

  override def getRuleNameAndcheckError(busi: BusinessWrapper): (String,String,util.List[String]) = {
    if ( hints == null ) return (null ,null,null)
    val ruleName = hints.get("rule")
    (hints.get("partitionkey"), ruleName) match {
      case (Some(partitionKey), Some(rule)) =>
        if (stmt.getTableElementList.find { defi =>
          defi.isInstanceOf[SQLColumnDefinition] &&
            ( defi.asInstanceOf[SQLColumnDefinition].getName().getSimpleName == partitionKey || defi.asInstanceOf[SQLColumnDefinition].getName().getSimpleName == s"`${partitionKey}`" )
        } == None) {
          throw new JobHandlerException("parition key not exists in the table columns!")
        }
        return (rule, partitionKey, busi.busiData.mysqls)
      case (None, None) =>
        return (null, null,null)
      case _ => throw new JobHandlerException("hints errors!")
    }
  }

  override def handle(busi: BusinessWrapper) : Unit = {
    super.handle(busi)
    val (ruleName, partitionKey,mysqls) = getRuleNameAndcheckError(busi)
    val element = stmt.getTableElementList.find{
      case cd: SQLColumnDefinition => cd.isAutoIncrement
      case _ => false
    }
    var autoIncrementColumn: String = null
    element.foreach { el =>
      autoIncrementColumn = el.asInstanceOf[SQLColumnDefinition].getName.getSimpleName
      if ( autoIncrementColumn.charAt(0) == '`') autoIncrementColumn = autoIncrementColumn.substring(1, autoIncrementColumn.size -1)
    }


    val table = new Table(partitionKey, ruleName, mysqls, autoIncrementColumn)
    ScadbConf.client.create.forPath(s"${ScadbConf.basePath}/businesses/$business/tables/" + tableName, new Gson().toJson(table).getBytes())
  }
}
