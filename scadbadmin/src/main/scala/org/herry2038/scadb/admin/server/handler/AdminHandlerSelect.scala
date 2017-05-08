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

import com.alibaba.druid.sql.ast.{SQLExpr, SQLStatement}
import com.alibaba.druid.sql.ast.expr.{SQLPropertyExpr, SQLVariantRefExpr, SQLValuableExpr, SQLIdentifierExpr}
import com.alibaba.druid.sql.ast.statement.{SQLExprTableSource, SQLSelectStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock
import org.herry2038.scadb.admin.ConnectionHelper
import org.herry2038.scadb.admin.leader.AdminMaster
import org.herry2038.scadb.admin.server.{AdminException, AdminConf}
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.db.util.SysVariables
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.util.DbUtils
import scala.collection.JavaConversions._

/**
 * Created by Administrator on 2016/9/1.
 */
class AdminHandlerSelect(statement: SQLStatement, handler: MySQLServerConnectionHandler) extends AdminHandlerInterface(statement, handler) {

  val stmt = statement.asInstanceOf[SQLSelectStatement]
  val tableName = Option(stmt.getSelect().getQuery().
    asInstanceOf[MySqlSelectQueryBlock].getFrom().
    asInstanceOf[SQLExprTableSource]).map(_.getExpr.asInstanceOf[SQLIdentifierExpr].getName).getOrElse(null)


  override def handle = {
    val metaBuf = new StringBuilder()
    val dataBuf = new StringBuilder()

    tableName match {
      case "assigns" =>
        metaBuf.append("ASSIGN BUSINESS")
        AdminConf.jobsManager.businesses.foreach(kv=>dataBuf.append(kv._1).append('\n'))
        if ( dataBuf.length > 0 )
          dataBuf.delete(dataBuf.length() - 1, dataBuf.length)
      case "master" =>
        metaBuf.append("MASTER")
        dataBuf.append(new String(ScadbConf.client.getData.forPath(s"${ScadbConf.basePath}/admins/nodes")))
      case "all_assigns" =>
        val current_master = new String(ScadbConf.client.getData.forPath(s"${ScadbConf.basePath}/admins/nodes"))
        if ( AdminMaster.isTrueMaster ) {
          metaBuf.append("node|businesses")
          AdminMaster.nodes.foreach{ kv =>
            dataBuf.append(kv._1).append('|').append(kv._2).append('\n')
          }
          if ( dataBuf.length > 0 )
            dataBuf.delete(dataBuf.length() - 1, dataBuf.length)
        } else if ( current_master == AdminConf.node ){
          throw new AdminException(s"please wait for a moment ${current_master}")
        } else {
          val pool = ConnectionHelper.getNodePool(current_master)

          try {
            val result = DbUtils.executeQuery(pool, "select * from all_assigns_for_master")
            metaBuf.append("node|businesses")
            result.foreach{ row =>
              row.foreach(dataBuf.append(_).append('|'))
              dataBuf.append('\n')
            }

            if ( dataBuf.length > 0 )
              dataBuf.delete(dataBuf.length() - 1, dataBuf.length)
          } catch {
            case ex: Throwable =>
              throw new AdminException(ex.getMessage)
          }
        }
      case "all_assigns_for_master" =>
        if ( AdminMaster.isTrueMaster ) {
          metaBuf.append("node|businesses")
          AdminMaster.nodes.foreach{ kv =>
            dataBuf.append(kv._1).append('|').append(kv._2).append('\n')
          }
          if ( dataBuf.length > 0 )
            dataBuf.delete(dataBuf.length() - 1, dataBuf.length)
        } else {
          val current_master = new String(ScadbConf.client.getData.forPath(s"${ScadbConf.basePath}/admins/nodes"))
          throw new AdminException(s"please concat the master node: ${current_master}")
        }
      case null => // 查询系统变量
        stmt.getSelect().getQuery().
          asInstanceOf[MySqlSelectQueryBlock].getSelectList.map { selectItem =>
          metaBuf.append(Option(selectItem.getAlias).getOrElse(selectItem.getExpr.toString())).append('|')
          dataBuf.append(getValue(selectItem.getExpr())).append('|')
        }
        metaBuf.delete(metaBuf.length() - 1, metaBuf.length)
        dataBuf.delete(dataBuf.length() - 1, dataBuf.length)
      case _ =>
        throw new AdminException(s"unknown system table ${tableName}")
    }
    val meta = metaBuf.toString
    val data = dataBuf.toString
    handler.write((meta, data))
  }

  def getValue(expr: SQLExpr): String = {
    if ( expr.isInstanceOf[SQLValuableExpr]) {
      return expr.asInstanceOf[SQLValuableExpr].getValue().toString()
    } else if ( expr.isInstanceOf[SQLVariantRefExpr]) {
      return SysVariables.getVariableValue(expr.asInstanceOf[SQLVariantRefExpr].getName().substring(2))
    } else if ( expr.isInstanceOf[SQLPropertyExpr] ) {
      val propertyExpr = expr.asInstanceOf[SQLPropertyExpr]
      if ( propertyExpr.getOwner.isInstanceOf[SQLVariantRefExpr] ) {
        return SysVariables.getVariableValue(propertyExpr.getName)
      }
    }
    throw new AdminException("unknown sys expr")
  }
}
