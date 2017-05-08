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
import java.util

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.{SQLDropTableStatement, SQLAlterTableStatement, SQLCreateTableStatement}
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser
import com.alibaba.druid.sql.parser.SQLStatementParser
import org.herry2038.scadb.admin.JobHandlerException
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.ScadbConf.Rule
import org.herry2038.scadb.conf.algorithm.Algorithm

import scala.util.{Failure, Success}

/**
 * Created by Administrator on 2016/11/29.
 */
object tools {
  def getAlgorithm(r: Rule): Algorithm = {
    val className = "org.herry2038.scadb.conf.algorithm." + r.algorithm
    try {
      val algorithm = Class.forName(className).newInstance().asInstanceOf[Algorithm]
      if ( algorithm.init(r) )
        return algorithm
      else
        throw new JobHandlerException("init algorithm %s error!", r.algorithm)
    } catch {
      case any: Throwable =>
        throw any
    }
  }

  def setTableName(statement: SQLStatement, newName: String): Unit = {
    statement match {
      case stmt: SQLCreateTableStatement =>stmt.getTableSource().getExpr.asInstanceOf[SQLIdentifierExpr].setName(newName)
      case stmt: SQLAlterTableStatement => stmt.getTableSource.getExpr.asInstanceOf[SQLIdentifierExpr].setName(newName)
      case stmt: SQLDropTableStatement => stmt.getTableSources.get(0).getExpr.asInstanceOf[SQLIdentifierExpr].setName(newName)
      case _ => throw new JobHandlerException("unknown sql type!!!")
    }
  }

  def getTableName(statement: SQLStatement): String = {
    statement match {
      case stmt: SQLCreateTableStatement =>
        val name = stmt.getTableSource.getExpr.asInstanceOf[SQLIdentifierExpr].getName()
        if ( name.charAt(0) == '`' )
          name.substring(1, name.length() - 1)
        else
          name
      case stmt: SQLAlterTableStatement => stmt.getTableSource.getExpr.asInstanceOf[SQLIdentifierExpr].getName()
      case stmt: SQLDropTableStatement => stmt.getTableSources.get(0).getExpr.asInstanceOf[SQLIdentifierExpr].getName
      case _ => throw new JobHandlerException("unknown sql type!!!")
    }
  }

  def main (args: Array[String]) {
    val rule = new Rule("HashIntAlgorithm" , 100)
    val algorithm = getAlgorithm(rule)
    val sql = "alter table tb_ms_video_collect_day_20161130 engine=tokudb"
    val parser: SQLStatementParser = new MySqlStatementParser(sql)
    val statement: SQLStatement = parser.parseStatement

    val tableName = getTableName(statement)

    val mysqls = new util.ArrayList[String]() ;
    mysqls.add("default")
    algorithm.partitions(tableName, mysqls, "videoday", 10).foreach { i =>
      setTableName(statement, i._2)
      val sql = statement.toString()
      println(sql + ";")
    }
  }
}
