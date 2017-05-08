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
package org.herry2038.scadb.conf

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import com.google.gson.Gson

import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener, PathChildrenCacheListener, PathChildrenCache}
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.herry2038.scadb.util.{Config, Logging}

import scala.collection.mutable.ArrayBuffer



object ScadbConf extends Logging {
  val lock = new ReentrantLock()

  // BUSINESS PATH:  ${businessesPath}/${business}
  // CLASTER PATH:   ${clustersPath}/${business}/${cluster_name1}
  //                 ${clustersPath}/${business}/${cluster_name2}

  var basePath = "/scadb"
  var businessesPath = "/scadb/businesses"
  var clustersPath = "/scadb/clusters"

  var backuser = "scadb"
  var backpass = "scadb"

  class Business(val default: String,
                 val mysqls: util.List[String],
                 val dbNums: Int = 0,
                 val dbPrefix: String = null,
                 val minConnections: Int=100,
                 val maxConnections: Int=100,
                 val charset: String = "utf8")

  class Rule(val algorithm: String, val partitions: Int = 0 , val subRules: util.List[String] = null, val scheduledPolicy: String = null)
  class Table(val column: String, val ruleName: String = null, val mysqls: util.List[String] = null, val uniqIdCol: String = null)
  class User(val user: String, val passwd: String, val privilege: Int)

  def objectToString[T](obj: T): String = new Gson().toJson(obj)

  //def stringToObject[T](str: String): T = new Gson().fromJson[T](str, new TypeToken[T](){}.getType()).asInstanceOf[T]

  def wrapperWithLock(f: ()=> Unit) = {
    lock.lock() ;
    try {
      f()
    } finally {
      lock.unlock
    }
  }

  object StateChangeListener extends ConnectionStateListener {
    val listeners: ArrayBuffer[ConnectionStateListener] = new ArrayBuffer[ConnectionStateListener]

    def register(listener: ConnectionStateListener) = listeners += listener

    def unregister(listener: ConnectionStateListener) = listeners -= listener

    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
      info("current zookeeper state :" + newState) ;
      listeners.foreach(_.stateChanged(client, newState))
    }
  }

  def pathCache(path: String, listener: PathChildrenCacheListener): PathChildrenCache = {
    val pathCache = new PathChildrenCache(client, path, true, false, executorServiceForZk)
    pathCache.start()
    pathCache.getListenable.addListener(listener, executorServiceForZk)
    pathCache
  }

  def dataCache(path: String, listener: NodeCacheListener): NodeCache = {
    val dataCache = new NodeCache(client, path)
    dataCache.getListenable.addListener(listener, executorServiceForZk)
    dataCache.start()
    dataCache
  }

  val executorServiceForZk = Executors.newFixedThreadPool(2)
  var client: CuratorFramework = _

  def start(zkPath: String): CuratorFramework = start(zkPath, Config.instance)

  def start(zkPath: String, conf: Config): CuratorFramework = {
    conf.getString("scadb_zk_base_path").foreach { p =>
      basePath = p
      businessesPath = s"${basePath}/businesses"
      clustersPath = s"${basePath}/clusters"
    }
    conf.getString("scadb_zk_clusters_path").foreach {clustersPath = _ }
    conf.getString("backuser").foreach { backuser = _ }
    conf.getString("backpass").foreach { backpass = _ }

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    client = CuratorFrameworkFactory.newClient(zkPath, retryPolicy)
    client.start()
    client.getConnectionStateListenable.addListener(StateChangeListener)
    client
  }

  def stop() = {
    client.close
    client = null
  }

}

