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

import java.util
import java.util.concurrent.{TimeUnit, Executors}

import com.alibaba.druid.sql.ast.SQLStatement
import org.apache.commons.dbcp.BasicDataSource
import org.herry2038.scadb.admin.model.BusinessWrapper
import org.herry2038.scadb.admin.server.AdminConf
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.ScadbConf.{Rule, Table}
import org.herry2038.scadb.conf.algorithm.Algorithm
import org.herry2038.scadb.db.util.ExecutorServiceUtils
import com.google.gson.Gson
import org.herry2038.scadb.util.{DbUtils, Log}


import scala.collection.mutable
import scala.collection.JavaConversions._


object JobHandler {
  val log = Log.get[JobHandler]
}

abstract class JobHandler(val business: String, val statement: SQLStatement, val hints: mutable.HashMap[String, String]) {
  implicit val internalPool = ExecutorServiceUtils.CachedExecutionContext
  import JobHandler.log

  def tableName: String
  def getRuleNameAndcheckError(busi: BusinessWrapper) : (String, String, util.List[String]) = {
    try {
      val table = new Gson().fromJson(
        new String(ScadbConf.client.getData().forPath(s"${ScadbConf.basePath}/businesses/$business/tables/" + tableName)),
        classOf[Table])
      return (table.ruleName, table.column, table.mysqls)
    } catch {
      case ex: Exception =>
        log.error(s"get $business 's table $tableName error!", ex)
        throw new JobHandlerException("get rule error: ", ex.getMessage)
    }
  }

  def handleSingle(busi: BusinessWrapper): Unit = {
    val pool = ConnectionHelper.getServicePool(busi.name, busi.busiData.default)
    try {
      DbUtils.executeUpdateSingleStatement(pool, statement.toString())
    } catch {
      case ex: Throwable =>
        throw new JobHandlerException("execute job error:", ex.getMessage)
    }
  }

  def handle(busi: BusinessWrapper) : Unit = {
    val (ruleName, _, mysqls) = getRuleNameAndcheckError(busi)
    if ( ruleName == null ) {
      return handleSingle(busi)
    }
    val rule = busi.getRule(ruleName)
    if ( rule == null )  {
      throw new JobHandlerException("rule %s not exists!", ruleName)
    }

    log.info(s"do with rule ${ruleName}")
    val algorithm = getAlgorithm(rule)

    val pools = mysqls.foldLeft(Map[String, BasicDataSource]()) { (hashmap, mysql) =>
      hashmap + (mysql->ConnectionHelper.getServicePool(busi.name, mysql))
    }

    val executor = Executors.newFixedThreadPool(AdminConf.parallelDegree);
    var code = 0
    var errmsg: String = "success"

    algorithm.partitions(tableName, mysqls, busi.busiData.dbPrefix, busi.busiData.dbNums).foreach { i =>
      setTableName(i._2)
      val sql = statement.toString()
      val server = i._3
      executor.execute(new Runnable() {
        override def run(): Unit = {
          log.info(s"executing on business:$business server:${server} with sql: $sql")
          pools.get(server) match {
            case Some(pool) =>
              try {DbUtils.executeUpdateSingleStatement(pool, sql) }
              catch {
                case e: Exception =>
                  JobHandler.log.error(s"execute on server :${server} sql: $sql failed!", e)
                  this.synchronized {
                    if (code == 0) {
                      errmsg = e.getMessage
                      code = -1
                    }
                  }
              }
            case None =>
              this.synchronized {
                JobHandler.log.error(s"cannot find pool: ${server} sql: ${sql}")
                if ( code == 0 ) {
                  code = -1
                  errmsg = "cannot find pool:" + i._3
                }
              }
          }
        }
      })
    }
    executor.shutdown()
    executor.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)

    pools.foreach (_._2.close)

    if ( code != 0 ) {
      throw new JobHandlerException(errmsg)
    }
  }

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
        log.info("get rule $r.algorithm error!", any)
        throw any
    }
  }

  def setTableName(tableName: String): Unit
}
