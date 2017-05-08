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
package org.herry2038.scadb.scadb.server.processor

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.statement._
import com.alibaba.druid.sql.dialect.mysql.ast.statement._
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import com.alibaba.druid.sql.parser.SQLStatementParser
import org.herry2038.scadb.mysql.message.server.ErrorMessage
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.newdecorator._
import org.herry2038.scadb.util.Log

class ScadbProcessor {

  import ScadbProcessor.log

  def handle(sql: String, handler: MySQLServerConnectionHandler): Unit = {
    //log.debug("Get a sql :{}", sql)
    Statistics.counter()
    val currentTime = System.nanoTime()
    try {
      val parser: SQLStatementParser = new MySqlStatementParser(sql)
      val sqlStatements = parser.parseStatementList()
      val decorator = ScadbProcessor.getDecorator(sql, sqlStatements, currentTime)
      if (decorator == null) {
        log.error(s"unsupported sql: ${sql}")
        handler.write(new ErrorMessage(-1, s"unsupported sql!"))
      } else {
        val statement = decorator.decorator(handler)
        if (statement == null) {
          decorator.writeError(handler)
        } else
          statement.executeEnclose()
      }
    } catch {
      case e: Throwable =>
        log.info(String.format("handle sql:%s error!", sql), e)
        val msg = new ErrorMessage(-1, Option(e.getMessage).getOrElse("null exception!!!"))
        handler.write(msg)
    }
  }
}

object ScadbProcessor {
  val log = Log.get[ScadbProcessor]

  //val ddlListener = new DdlJobsListener

  def getDecorator(sql: String, statements: java.util.List[SQLStatement], startTime: Long): DecoratorInterface = {
    if (statements.size() > 1) {
      for (i <- 0 until statements.size() - 1) {
        if (!statements.get(i).isInstanceOf[MySqlHintStatement]) {
          return null
        }
      }
    } else if (statements.isEmpty) {
      return null
    }
    statements.get(statements.size() - 1) match {
      case _: SQLDDLStatement =>
        if ((ScadbConfig.conf.loggingSql & 4) != 0) {
          log.info("GRABBEDSQL :{}", sql)
        }
        return new DecoratorInterfaceDdl(sql, statements, startTime)
      case _: MySqlLoadDataInFileStatement =>
        if ((ScadbConfig.conf.loggingSql & 2) != 0) {
          log.info("GRABBEDSQL :{}", sql)
        }
        return new DecoratorInterfaceLoad(sql, statements, startTime)
      case _ =>
        getDecoratorCommon(sql, statements.get(statements.size() - 1), startTime)
    }
  }

  def getDecoratorCommon(sql: String, statement: SQLStatement, startTime: Long): DecoratorInterface = {
    if (statement.isInstanceOf[SQLSelectStatement]) {
      if ((ScadbConfig.conf.loggingSql & 1) != 0) {
        log.info("GRABBEDSQL :{}", sql)
      }

      val select = statement.asInstanceOf[SQLSelectStatement].getSelect.getQuery
      if (select.isInstanceOf[MySqlSelectQueryBlock] && select.asInstanceOf[MySqlSelectQueryBlock].getInto != null)
        return new DecoratorInterfaceSelectInto(statement, startTime)
      return new DecoratorInterfaceQuery(statement, startTime)
    } else if (statement.isInstanceOf[SQLInsertStatement]) {
      if ((ScadbConfig.conf.loggingSql & 2) != 0) {
        log.info("GRABBEDSQL :{}", sql)
      }
      return new DecoratorInterfaceInsert(statement, startTime)
    } else if (statement.isInstanceOf[SQLUpdateStatement]) {
      if ((ScadbConfig.conf.loggingSql & 2) != 0) {
        log.info("GRABBEDSQL :{}", sql)
      }
      return new DecoratorInterfaceUpdate(statement, startTime)
    } else if (statement.isInstanceOf[SQLDeleteStatement]) {
      if ((ScadbConfig.conf.loggingSql & 2) != 0) {
        log.info("GRABBEDSQL :{}", sql)
      }
      return new DecoratorInterfaceDelete(statement, startTime)
    } else if (statement.isInstanceOf[MySqlShowVariantsStatement]) {
      return new DecoratorInterfaceDefaultPool(sql, statement, startTime)
    } else if (statement.isInstanceOf[MySqlReplaceStatement]) {
      if ((ScadbConfig.conf.loggingSql & 2) != 0) {
        log.info("GRABBEDSQL :{}", sql)
      }
      return new DecoratorInterfaceReplace(statement, startTime)
    }
    return new DecoratorInterfaceSys(sql, statement)
  }
}
