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
package org.herry2038.scadb.scadb.conf

import org.herry2038.scadb.conf.ScadbConf.{Rule, User, Table, Business}
import org.herry2038.scadb.conf.cluster.ClusterConf
import org.herry2038.scadb.conf.{ScadbConfListener, ScadbConf}
import org.herry2038.scadb.conf.algorithm.Algorithm
import org.herry2038.scadb.conf.business.ScadbConfBusiness
import org.herry2038.scadb.conf.jobs.JobsManager
import org.herry2038.scadb.scadb.handler.ddl.DdlJobsListener
import org.herry2038.scadb.util.{Config, Log}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.collection.mutable
import java.util

class ScadbConfig extends ScadbConfListener {
  val log = Log.get[ScadbConfig]

  var busiConf: ScadbConfBusiness = _
  var business: String = _
  var zoneid: String = _
  var zookeeper: String = _
  var busi: Business = _

  var loggingSql = 0
  var parallelDegree = 20

  var executeThreads = 16
  var backgroudThreads = 16
  var port = 8888

  var idleMaxWaitTime = 600000        // how long are objects going to be kept as idle (not in use by clients of the pool)
  var idleCheckPeriod = 30000          // 后端MySQL的idle连接检查周期， 检查MySQL连接的有效性
  var idleCheckConnectionsInPool = 10 // 缺省每个池子检查10个连接就返回
  var maxQueueSize = 1000             // 连接池中最多等待执行的队列长度

  val partitionRules = new ConcurrentHashMap[String, Algorithm]
  val tables = new ConcurrentHashMap[String, Table]
  val users = new ConcurrentHashMap[String, User]
  var whitelists = new java.util.HashSet[String]()
  val proxys = mutable.HashMap[String, ClusterConf]()

  def load() = {
    Config.loadConfig("/scadb.properties")
    log.info("loading config file scadb.properties ......")
    zookeeper = Config.instance.getString("zookeeper", "localhost:2181")
    business = Config.instance.getString("business", null)
    zoneid = Config.instance.getString("zoneid", null)
    port = Config.instance.getInt("port", 9527)
    executeThreads = Config.instance.getInt("executeThreads", 16)
    backgroudThreads = Config.instance.getInt("backgroudThreads", 16)

    idleMaxWaitTime = Config.instance.getInt("idleMaxWaitTime", idleMaxWaitTime)
    idleCheckPeriod = Config.instance.getInt("idleCheckPeriod", idleCheckPeriod)
    idleCheckConnectionsInPool = Config.instance.getInt("idleCheckConnectionsInPool", idleCheckConnectionsInPool)
    maxQueueSize = Config.instance.getInt("maxQueueSize", maxQueueSize)
    parallelDegree = Config.instance.getInt("parallelDegree", parallelDegree)

    Config.instance.getString("loggingSql").foreach { value =>
      var loggingFlags = 0
      value.split(",").foreach { t =>
        t match {
          case "query" => loggingFlags = loggingFlags | (1 << 0)
          case "dml" => loggingFlags = loggingFlags | (1 << 1)
          case "ddl" => loggingFlags = loggingFlags | (1 << 2)
          case "all" => loggingFlags = loggingFlags | 7
          case "clear" => loggingFlags = 0
        }
        this.loggingSql = loggingFlags
      }
    }

    ScadbConf.start(zookeeper)

    busiConf = new ScadbConfBusiness(business)
    busiConf.registerListener(this)
    busiConf.start()

    JobsManager.startJobResultsListener(DdlJobsListener)
  }

  /**
   *
   * @param rule: 算法
   * @param value：评估值
   * @return (分区号，对应的MySQL, 数据库前缀 )
   */
  def getPartitionAndServer(rule: Algorithm, value: String, mysqls: util.List[String]) = {
    val partitionAndSeq = rule.partition(value)

    if ( busi.dbNums > 0 ) {
      (partitionAndSeq._1, mysqls.get(partitionAndSeq._2 % mysqls.size), busi.dbPrefix + (partitionAndSeq._2 % busi.dbNums) )
    } else {
      (partitionAndSeq._1, mysqls.get(partitionAndSeq._2 % mysqls.size), null)
    }
  }

  def getServer(partition: String, mysqls: util.List[String]) = {
    val partitionNo = partition.toInt
    if ( busi.dbNums > 0 ) {
      (partition, mysqls.get(partitionNo % mysqls.size), busi.dbPrefix + (partitionNo % busi.dbNums) )
    } else {
      (partition, mysqls.get(partitionNo % mysqls.size), null)
    }
  }

  override def uptBusiness(business: String, busi: Business): Unit = {

    this.busi = new Business(
      busi.default, busi.mysqls, busi.dbNums, busi.dbPrefix,
      if ( busi.minConnections <= 0 ) 100 else busi.minConnections,
      if ( busi.maxConnections <= 0 ) 100 else busi.maxConnections,
      if ( busi.charset == null ) "utf8" else busi.charset)


    this.busi.mysqls.map { mysql =>
      if ( !proxys.contains(mysql) ) {
        val setAndProxy = mysql.split("\\.")
        val proxy = new ClusterConf(ClusterConf.path(setAndProxy(0), setAndProxy(1)),setAndProxy(1))
        proxy.registerListener(ScadbMySQLPools)
        proxys += (mysql -> proxy)
      }
    }

    proxys.dropWhile { kv =>
      val exists = this.busi.mysqls.exists(_ == kv._1)
      if ( !exists ) {
        kv._2.close()
      }
      ! exists
    }
  }

  override def addRule(business: String, rule: String, r: Rule): Unit = {
    val className = "org.herry2038.scadb.conf.algorithm." + r.algorithm
    try {
      val algorithm = Class.forName(className).newInstance().asInstanceOf[Algorithm]
      if ( algorithm.init(r) )
        partitionRules.putIfAbsent(rule, algorithm)
      else
        log.info(String.format("add rule [%s] for business [%s] init failed!", rule, business))
    } catch {
      case any: Throwable =>
        log.info(String.format("add rule [%s] for business [%s] failed [%s]!", rule, business, any))
    }
  }

  override def delTable(business: String, table: String): Unit = {
    tables.remove(table)
  }

  override def uptTable(business: String, table: String, t: Table): Unit = {
    tables.replace(table, t)
  }

  override def delRule(business: String, rule: String): Unit = {
    partitionRules.remove(rule)
  }

  override def uptRule(business: String, rule: String, r: Rule): Unit = {
    val className = "org.herry2038.scadb.conf.algorithm." + r.algorithm
    try {
      val algorithm = Class.forName(className).newInstance().asInstanceOf[Algorithm]
      if ( algorithm.init(r) )
        partitionRules.replace(rule, algorithm)
      else
        log.info(String.format("add rule [%s] for business [%s] init failed!", rule, business))
    } catch {
      case any: Throwable =>
        log.info(String.format("add rule [%s] for business [%s] failed [%s]!", rule, business, any))
    }
  }
  override def addTable(business: String, table: String, t: Table): Unit = tables.putIfAbsent(table, t)
  override def addUser(business: String, user: String, u: User): Unit = users.putIfAbsent(user, u)
  override def uptUser(business: String, user: String, u: User): Unit = users.put(user, u)
  override def delUser(business: String, user: String): Unit = users.remove(user)

  override def uptWhitelist(business: String, whitelist: java.util.List[String]): Unit = whitelists = new java.util.HashSet[String](whitelist)
}

object ScadbConfig {
  val conf = new ScadbConfig
  def load() = conf.load()
}
