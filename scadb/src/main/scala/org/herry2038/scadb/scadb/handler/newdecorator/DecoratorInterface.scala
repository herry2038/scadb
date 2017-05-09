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

import com.alibaba.druid.sql.ast.expr._
import com.alibaba.druid.sql.ast.{SQLExpr, SQLStatement}
import org.herry2038.scadb.conf.ScadbConf.Table
import org.herry2038.scadb.conf.algorithm.Algorithm
import org.herry2038.scadb.mysql.codec.MessageTrack
import org.herry2038.scadb.mysql.message.server.ErrorMessage
import org.herry2038.scadb.db.util.SysVariables
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatement, ScadbStatementMulti, ScadbStatementSingle}
import org.herry2038.scadb.scadb.server.processor.{ScadbUuid, Statistics}
import org.herry2038.scadb.util.Log

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._


abstract class DecoratorInterface(val statement: SQLStatement, val startTime: Long) {
  def readable(): Boolean
  def tableName(): String
  def requestType = Statistics.OTHER_TYPE

  protected var rule: Algorithm = null

  protected var table: Table = _

  protected var errno: Int = -1
  protected var errmsg: String = "cannot find the partition!"

  protected def getValue(e: SQLExpr, handler: MySQLServerConnectionHandler): String = e match {
    case  expr:SQLValuableExpr =>
      expr.getValue().toString()
    case expr:SQLVariantRefExpr =>
      val name = expr.getName().substring(2)
      if ( name == "uuid")
        ScadbUuid.uuid.toString
      else
        SysVariables.getVariableValue(name)
    case expr:SQLPropertyExpr if expr.getOwner.isInstanceOf[SQLVariantRefExpr] =>
      SysVariables.getVariableValue(expr.getName)
    case expr: SQLMethodInvokeExpr if expr.getMethodName.equalsIgnoreCase("last_insert_id") =>
      handler.lastInsertId.toString
    case _ =>
      throw new RuntimeException("unknown sys expr")
  }

  def decorator(handler: MySQLServerConnectionHandler, mt: MessageTrack) : ScadbStatement = new ScadbStatementSingle(handler, statement.toString(), ScadbConfig.conf.busi.default, startTime, requestType, readable() & handler.isReadWriteSplit, mt)

  def decorator(handler: MySQLServerConnectionHandler) : ScadbStatement = new ScadbStatementSingle(handler, statement.toString(), ScadbConfig.conf.busi.default, startTime, requestType, readable() & handler.isReadWriteSplit)

  def setPartition(partition: String, prefix: String): Unit

  def writeError(handler: MySQLServerConnectionHandler): Unit = {
    val msg = new ErrorMessage(errno, errmsg)
    handler.write(msg)
  }

  def getTable(t: String) : Boolean  = {
    table = ScadbConfig.conf.tables.get(t)
    if ( table == null ) {
      errno = -1
      errmsg = "cannot find table:" + t
      return false
    }
    if ( table.ruleName != null ) {
      rule = ScadbConfig.conf.partitionRules.get(table.ruleName)
      if (rule == null) {
        errno = -1
        errmsg = "cannot find table:" + t + "'s rule:" + table.ruleName
        return false
      }
    }
    true
  }


  def decoratorWhere(handler: MySQLServerConnectionHandler, where: SQLExpr): ScadbStatement = where match {
    case binaryCondi: SQLBinaryOpExpr =>
      if (binaryCondi.getOperator == SQLBinaryOperator.Equality && binaryCondi.getLeft().isInstanceOf[SQLIdentifierExpr] && binaryCondi.getLeft().asInstanceOf[SQLIdentifierExpr].getName() == table.column ) {
        if ( binaryCondi.getRight().isInstanceOf[SQLValuableExpr] ) {
          val result = ScadbConfig.conf.getPartitionAndServer(rule, binaryCondi.getRight().asInstanceOf[SQLValuableExpr].getValue().toString(), table.mysqls)
          setPartition(result._1, result._3)
          new ScadbStatementSingle(handler, statement.toString(), result._2, startTime, requestType, readable && handler.isReadWriteSplit)
        } else null
      } else if ( binaryCondi.getOperator == SQLBinaryOperator.BooleanAnd ) {
        decoratorWhere(handler, binaryCondi.getLeft) match {
          case null => decoratorWhere(handler, binaryCondi.getRight)
          case statement: ScadbStatement => statement
        }
      } else
        null
    case inCondi: SQLInListExpr =>
      if ( inCondi.getExpr.isInstanceOf[SQLIdentifierExpr] && inCondi.getExpr.asInstanceOf[SQLIdentifierExpr].getName() == table.column ) {
        val finalPartitions = inCondi.getTargetList.foldLeft(new HashMap[String,(String,String,java.util.List[SQLExpr])]) { // ( MySQL服务,  数据库前缀, 同一个分区中的分区键 )
          (partitions,currentItem) =>
            if ( !currentItem.isInstanceOf[SQLValuableExpr]) return null
            val (partition,server, prefix) = ScadbConfig.conf.getPartitionAndServer(rule, currentItem.asInstanceOf[SQLValuableExpr].getValue.toString, table.mysqls)
            val obj = partitions.get(partition) match {
              case Some((server, prefix, items)) =>
                items.add(currentItem)
              case None =>
                val items = new java.util.ArrayList[SQLExpr]
                items.add(currentItem)
                partitions += (partition -> (server, prefix, items))
            }
            partitions
        }
        val sqls = new java.util.ArrayList[(String,String,MessageTrack)]

        finalPartitions.foreach {
          case ( k:String, v ) => // v 的类型：(String,String,java.util.List[HiveItem])
            setPartition(k, v._2)
            inCondi.setTargetList(v._3)
            sqls.add((v._1, statement.toString(), null))
        }
        return new ScadbStatementMulti(handler,sqls,
          startTime, requestType,
          readable && handler.isReadWriteSplit)
      } else
        null
    case _ =>
      null
  }
}

object DecoratorInterface {
  val log = Log.get[DecoratorInterface]
  val ONE_TIME_SIZE = 41943040
  val hintsPrefix = "/*!scadb:"
}