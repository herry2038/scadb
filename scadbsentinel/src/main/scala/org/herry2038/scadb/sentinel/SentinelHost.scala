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

package org.herry2038.scadb.sentinel

import java.sql.{SQLException, ResultSet, Statement}

import com.google.gson.Gson
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.cluster.{Status, ClusterConf}
import org.herry2038.scadb.conf.cluster.ClusterModel.MySQLStatus
import org.herry2038.scadb.util.Logging


class SentinelHost(val cluster: String, val instance: String, mysqlStatus: MySQLStatus) extends Logging {
  private var lastDetectTimestamp = System.currentTimeMillis()
  private var lastStateChangeTimestamp = System.currentTimeMillis()
  private var lastMySQLStatus: MySQLStatus = mysqlStatus
  private var failedDetectorInterval = SentinelConf.detectorInterval

  private val FAILED_STATUS = new MySQLStatus(mysqlStatus.zoneid, mysqlStatus.role, Status.FAILED, -1)
  private val SUCCEED_STATUS = new MySQLStatus(mysqlStatus.zoneid, mysqlStatus.role, Status.START, -1)

  private var suspected = false

  private val zkUrl = {
    val parts = cluster.split("\\.")
    ClusterConf.path(parts(0), parts(1)) + "/" + instance
  }


  private val mysqlUrl = s"jdbc:mysql://${instance}/?connectTimeout=5000&socketTimeout=5000&jdbcCompliantTruncation=false&useUnicode=true&characterEncoding=UTF-8"
  private val mysqlConnection = new MySQLConnection(mysqlUrl)

  private def setLastStatus(dbStatus: MySQLStatus) = {
    this.lastMySQLStatus = dbStatus
    lastStateChangeTimestamp = System.currentTimeMillis()
  }

  def detect(timestamp: Long): Unit = {
    // 探测
    lastDetectTimestamp = timestamp
    val status = detectorMySQL

    if (lastMySQLStatus.status == Status.FAILED) {
      if (status.status == Status.FAILED) {
        failedDetectorInterval = Math.min(failedDetectorInterval * 2, SentinelConf.maxFailedDetectorInterval)
      } else if (status.status == Status.START) {
        failedDetectorInterval = SentinelConf.detectorInterval
      }
    }

    // 检查是否需要同步到ZK
    val (delaySeconds: Int, isNeedChangeZk: Boolean) = needChangeZk(status)

    if ( isNeedChangeZk ) {
      lastMySQLStatus = new MySQLStatus(status.zoneid, status.role, status.status, delaySeconds)
      try {
        ScadbConf.client.setData().forPath(zkUrl, new Gson().toJson(lastMySQLStatus).getBytes())
      } catch {
        case ex: Exception =>
          error(s"sync to zk url: ${zkUrl} error!", ex)
      }
    }
  }


  def needChangeZk(status: MySQLStatus): (Int, Boolean) = {
    if (status.status != lastMySQLStatus.status) {
      val delaySeconds = if (status.delaySeconds == -1) lastMySQLStatus.delaySeconds else status.delaySeconds
      if (lastMySQLStatus.status == Status.START && status.status == Status.FAILED) {
        if (!suspected) {
          suspected = true
          return (delaySeconds, false)
        } else {
          suspected = false
        }
      }
      return (delaySeconds,true)
    } else if (status.status == Status.START) {
      if (suspected) suspected = false
      if (status.delaySeconds == -1) return (-1,false)
      return (status.delaySeconds, Math.abs(status.delaySeconds - lastMySQLStatus.delaySeconds) > SentinelConf.slaveDelaySecondsChangeInterval )
    }
    (-1, false)
  }

  private def detectorMySQL: MySQLStatus = {
    info(s"detector ${cluster} - ${instance} ......")
    val con = mysqlConnection.getConnection

    if ( con == null ) {
      debug(s"detector ${cluster} - ${instance} failed!")
      return FAILED_STATUS
    }

    var stmt: Statement = null
    var rs : ResultSet = null

    try {
      stmt = con.createStatement()
      rs = stmt.executeQuery("show slave status")
      if ( rs.next() ) {
        val behindMaster = rs.getString("Seconds_Behind_Master")
        debug(s"detector ${cluster} - ${instance} succeed!")
        val behindMasterValue = if ( behindMaster == null || behindMaster.isEmpty ) -1 else behindMaster.toInt
        return new MySQLStatus(lastMySQLStatus.zoneid, lastMySQLStatus.role, Status.START, behindMasterValue)
      }
      debug(s"detector ${cluster} - ${instance} succeed!")
      return SUCCEED_STATUS

    } catch {
      case ex: SQLException =>
        error(s"detector ${cluster} - ${instance} failed!", ex)
        return FAILED_STATUS
    } finally {
      MySQLConnection.closeClosable(rs)
      MySQLConnection.closeClosable(stmt)
    }
  }

  def needDetector(timestamp: Long): Boolean = {
    if ( SentinelConf.zoneId == "*" || SentinelConf.zoneId == mysqlStatus.zoneid ) {
      if ( lastMySQLStatus == null ) {
        error(s"${cluster} - ${instance} status is null!!!")
        return false
      }
      lastMySQLStatus.status match {
        case Status.START => true
        case Status.STOP => false
        case Status.FAILED =>
          timestamp - lastDetectTimestamp > failedDetectorInterval
      }
    } else
      false
  }

}
