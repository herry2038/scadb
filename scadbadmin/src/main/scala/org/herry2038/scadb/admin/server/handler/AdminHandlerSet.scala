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
package org.herry2038.scadb.admin.server.handler

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.expr.{SQLCharExpr, SQLVariantRefExpr}
import com.alibaba.druid.sql.ast.statement.SQLSetStatement
import org.herry2038.scadb.admin.server.{AdminException, AdminCommandProcessor, AdminConf}
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler

class AdminHandlerSet(statement: SQLStatement, handler: MySQLServerConnectionHandler) extends AdminHandlerInterface(statement, handler) {
  val stmt = statement.asInstanceOf[SQLSetStatement]


  override def handle: Unit = {
    if ( stmt.getItems.size != 1 ) throw new AdminException("unspport set multi value!!!")
    val assignItem = stmt.getItems.get(0)


    if ( assignItem.getTarget.isInstanceOf[SQLVariantRefExpr]) {
      val name = assignItem.getTarget.asInstanceOf[SQLVariantRefExpr].getName
      if ( name.equalsIgnoreCase("autocommit")) {
        AdminCommandProcessor.writeOkMsg(handler)
        return
      }
      if ( assignItem.getValue.isInstanceOf[SQLCharExpr]) {
        var value = assignItem.getValue.toString

        if ( value.charAt(0) == '\'' || value.charAt(0) == '"') {
          value = value.substring(1, value.length() -1 )
        }
        name match {
          case "assign" => doWithAssign(value)
          case "unassign" => doWithUnAssign(value)
          case _ =>
            //throw new AdminException("unsupported set parameter!")
            AdminCommandProcessor.writeOkMsg(handler)
        }
      } else {
        AdminCommandProcessor.writeOkMsg(handler)
      }
    } else {
      throw new AdminException("set syntax set param_name='param_value'!!!")
    }
  }

  def doWithAssign(value: String): Unit = {
    AdminConf.jobsManager.assignBusiness(value) match {
      case (true, msg) =>
        AdminCommandProcessor.writeOkMsg(handler)
      case (false, msg) =>
        throw new AdminException(s"assign business:${value} error: ${msg}")
    }
  }

  def doWithUnAssign(value: String): Unit = {
    if (AdminConf.jobsManager.unassignBusiness(value)) {
      AdminCommandProcessor.writeOkMsg(handler)
    } else {
      AdminCommandProcessor.writeErrorMsg(handler, 1, s"business ${value} not exists!!!")
    }
  }
}
