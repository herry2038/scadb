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

import java.util.concurrent.{Executors, ConcurrentHashMap}


import org.herry2038.scadb.conf.cluster.ClusterConfListener
import org.herry2038.scadb.conf.cluster.ClusterModel.{MySQLStatus, MasterModel}
import org.herry2038.scadb.util.Logging
import scala.util.Try
import scala.collection.JavaConversions._

object SentinelDetectorService extends ClusterConfListener with Logging {
  val clusters = new ConcurrentHashMap[String, SentinelHost]
  lazy val detectorExecutor = Executors.newFixedThreadPool(SentinelConf.parallelDegree)

  override def delInstance(cluster: String, instance: String): Unit = clusters.remove(cluster + "." + instance)

  override def uptInstance(cluster: String, instance: String, status: MySQLStatus): Unit = {
    clusters.put(cluster + "." + instance, new SentinelHost(cluster, instance, status))
  }

  override def addInstance(cluster: String, instance: String, status: MySQLStatus): Unit = clusters.put(cluster + "." + instance, new SentinelHost(cluster, instance, status))

  override def uptMasterModel(cluster: String, cluterModel: MasterModel): Unit = None

  def detector: Unit = {
    val latch = new CountLatch(0)
    val timestamp = System.currentTimeMillis()
    val tasks = genTasks(timestamp, latch)
    latch.setCount(tasks.size)
    info(s"sentinel detect start , task size: ${tasks.size}")
    tasks.foreach(detectorExecutor.execute(_))

    Try(latch.await).recover{ case ex: InterruptedException =>
          if ( latch.count != 0 ) {
            ex.printStackTrace()
            System.exit(-1)
          }
    }
  }

  def genTasks(timestamp: Long, latch: CountLatch): List[Runnable] = {
    clusters.foldLeft(List[Runnable]()) { (list, kv) =>
      if ( kv._2.needDetector(timestamp) ) {
        val runnable =
          new Runnable {
            override def run(): Unit = {
              kv._2.detect(timestamp)
              latch.countDown
            }
          }
        runnable :: list
      } else {
        list
      }
    }
  }
}
