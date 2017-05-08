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

/**
 * Created by Administrator on 2016/3/10.
 */

import java.util.concurrent.{CountDownLatch, ConcurrentHashMap}

import org.apache.commons.dbcp.BasicDataSource
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.ScadbConf.Business
import org.herry2038.scadb.conf.cluster.ClusterModel
import org.herry2038.scadb.mysql.MySQLConnection
import org.herry2038.scadb.db.util.ExecutorServiceUtils
import org.herry2038.scadb.db.util.FutureUtils.awaitFuture
import org.herry2038.scadb.db._
import org.herry2038.scadb.db.pool.{PoolConfiguration}
import com.google.gson.Gson
import org.herry2038.scadb.util.{DbUtils, Log}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ConnectionHelper {
  class ConnectionHelperClass

  val pools = new ConcurrentHashMap[String, BasicDataSource]()
  val Default = new PoolConfiguration(20, 20, 5000, 10000)
  val log = Log.get[ConnectionHelperClass]

  implicit val internalPool = ExecutorServiceUtils.CachedExecutionContext
  def withConfigurableConnection[T]( configuration : Configuration )(fn : (Connection) => T) : Future[T] = {
    val connection = new MySQLConnection(0, configuration)
    connection.connect.map { connection =>
      val t= fn(connection)
      connection.disconnect
      t
    }
  }


  def executeQuery( connection : Connection, query : String  ) : QueryResult = {
    awaitFuture( connection.sendQuery(query) )
  }

  def executePreparedStatement( connection : Connection, query : String, values : Any * ) : QueryResult = {
    awaitFuture( connection.sendPreparedStatement(query, values) )
  }


  def getServiceConfiguration(businame: String, clusterName: String): Configuration = {
    val busiPath = ScadbConf.businessesPath + "/" + businame
    val business = new Gson().fromJson(new String(ScadbConf.client.getData().forPath(busiPath)), classOf[Business])

    val subPath = clusterName.replace('.', '/')
    val path = ScadbConf.clustersPath + "/" + subPath
    val value = new String(ScadbConf.client.getData().forPath(path))
    val masterModel = new Gson().fromJson(value, classOf[ClusterModel.MasterModel])

    val ipAndPort = masterModel.currentMaster.split(":")
    new Configuration(
      ScadbConf.backuser,
      ipAndPort(0),
      ipAndPort(1).toInt,
      password = Some(ScadbConf.backpass),
      database = Some(business.dbPrefix)
    )
  }

  def getServicePool(businame: String, service: String): BasicDataSource = {
    val conf = getServiceConfiguration(businame, service)
    DbUtils.createPool(conf.host + ":" + conf.port, conf.username, conf.password.get, conf.database, 500)
  }

  def remove(name: String): Unit = {
    val pool = pools.remove(name)
    if ( pool != null ) {
      pool.close
    }
  }

  def getNodeConfiguration(node: String): Configuration = {
    val ipAndPort = node.split(":")
    new Configuration(
      "myshard",
      ipAndPort(0),
      ipAndPort(1).toInt,
      password = Some("test"),
      database = None
    )
  }


  def getNodePool(node: String): BasicDataSource = {
    val conf = getNodeConfiguration(node)
    DbUtils.createPool(conf.host + ":" + conf.port, conf.username, conf.password.get, conf.database, 10)
  }

}

