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
package org.herry2038.scadb.admin

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.statement.{SQLDropTableStatement, SQLAlterTableStatement, SQLCreateTableStatement}
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import com.alibaba.druid.sql.parser.SQLStatementParser
import org.herry2038.scadb.admin.handlers.{JobHandlerCreateTable, JobHandlerAlterTable, JobHandlerDropTable}
import org.herry2038.scadb.admin.server.AdminConf
import scala.collection.mutable

object JobProcessor {
  val hintsPrefix = "/*!scadb:"

  def createHandler(business: String, statement: SQLStatement, hints: mutable.HashMap[String,String]) = {
    statement match {
      case _: SQLCreateTableStatement =>
        new JobHandlerCreateTable(business, statement, hints)
      case _: SQLAlterTableStatement =>
        new JobHandlerAlterTable(business, statement, hints)
      case _: SQLDropTableStatement =>
        new JobHandlerDropTable(business, statement, hints)
      case _ =>
        new JobHandlerCreateTable(business, statement, hints)
    }
  }

  def process(business: String, jobId: String, jobInfo: String): Unit = {
    var hints: mutable.HashMap[String,String] = null
    val sql =
    if ( jobInfo.startsWith(hintsPrefix) ) {
      val endPos = jobInfo.indexOf("*/");
      val hintSQL = jobInfo.substring(hintsPrefix.length(), endPos).trim()
      val hintStrs = hintSQL.split(" ")

      hints = new mutable.HashMap[String,String]()
      for ( hint <- hintStrs ) {
        val kv = hint.split("=")
        hints += (kv(0)-> kv(1))
      }
      jobInfo.substring(endPos + 2)
    } else jobInfo


    val parser: SQLStatementParser = new MySqlStatementParser(sql)
    val statement: SQLStatement = parser.parseStatement

    val handler = createHandler(business, statement, hints)

    val busi = AdminConf.businesses.get(business)
    if ( busi == null )
      throw new JobHandlerException(s"unknown business ${business}")
    handler.handle(busi)
  }

}
