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
package org.herry2038.scadb.admin.server

import com.alibaba.druid.sql.ast.statement.{SQLSetStatement, SQLSelectStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetNamesStatement
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import com.alibaba.druid.sql.parser.SQLStatementParser
import org.herry2038.scadb.admin.server.handler.{AdminHandlerSet, AdminHandlerSelect}
import org.herry2038.scadb.mysql.message.server.{OkMessage, ErrorMessage}
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.util.Logging

class AdminCommandProcessor extends Logging {
  import AdminCommandProcessor._
  def handle(sql: String, handler: MySQLServerConnectionHandler): Unit = {
    try {
      val parser: SQLStatementParser = new MySqlStatementParser(sql)

      val sqlStatements = parser.parseStatementList()
      if ( sqlStatements.size() > 1 ) {
        writeErrorMsg(handler, -1, "multi statement not supported!")
        return
      } else if ( sqlStatements.isEmpty ) {
        writeErrorMsg(handler, -1, "sql is empty!!!")
        return
      }
      val statement = sqlStatements.get(0)

      val adminHandler = statement match {
        case stmt: SQLSelectStatement => new AdminHandlerSelect(stmt, handler)
        case stmt: SQLSetStatement => new AdminHandlerSet(stmt, handler)
        case stmt: MySqlSetNamesStatement => {
          writeOkMsg(handler)
          return
        }
        case _ => {
          writeErrorMsg(handler, -1, "unsupported sql!!!")
          return
        }
      }

      adminHandler.handle
    } catch {
      case e: AdminException =>
        writeErrorMsg(handler, -1, e.getMessage)
      case e: Throwable =>
        info(String.format("handle sql:%s error!", sql), e)
        writeErrorMsg(handler, -1, Option(e.getMessage).getOrElse("null exception!!!"))
    }
  }
}


object AdminCommandProcessor {
  def writeErrorMsg(handler: MySQLServerConnectionHandler, code: Int, msg: String): Unit = {
    val error = new ErrorMessage(code, msg)
    handler.write(error)
  }

  def writeOkMsg(handler: MySQLServerConnectionHandler): Unit = {
    val ok = new OkMessage(0,0,0,0,null)
    handler.write(ok)
  }
}


