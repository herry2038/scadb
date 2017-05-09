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
package org.herry2038.scadb.admin.leader

import java.util
import java.util.{TimerTask, Timer}
import java.util.concurrent.CountDownLatch
import com.google.common.base.Preconditions
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.herry2038.scadb.admin.ConnectionHelper
import org.herry2038.scadb.admin.curator.JobsListeners
import org.herry2038.scadb.admin.server.AdminConf
import JobsListeners.{NodesListener}
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.util.{DbUtils, Log}

import scala.collection.mutable
import scala.util.{Failure, Success}
import ConnectionHelper._
import scala.collection.JavaConversions._

object AdminMaster {
  class AdminMaster
  val log = Log.get[AdminMaster]

  private val zkNodesPath = s"${ScadbConf.basePath}/admins/nodes"
  private var pathNodeCache: PathChildrenCache = null
  //private val zkNodeAssignsPath = s"${ScadbConf.basePath}/admins/assigns"

  var isTrueMaster = false

  val nodes = new mutable.HashMap[String, java.util.List[String]]()

  private val timer  = {
    val t = new Timer()
    t.schedule(new DetectorTimerTask(), 5000, 60000)
    t
  }


  def start: Unit = {
    ScadbConf.wrapperWithLock { () =>
      nodes.clear
      pathNodeCache = ScadbConf.pathCache(zkNodesPath, new NodesListener)
      ScadbConf.client.setData().forPath(s"${ScadbConf.basePath}/admins/nodes", AdminConf.node.getBytes())
      isTrueMaster = true
    }
  }

  def stop: Unit = {
    ScadbConf.wrapperWithLock { () =>
      isTrueMaster = false
      nodes.clear
      pathNodeCache.close
      pathNodeCache = null
    }
  }

  def removeNode(node: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      if (!isTrueMaster) return
      val list = nodes.remove(node).getOrElse(new util.ArrayList[String]())
      //ConnectionHelper.remove(node)
    }
  }

  def addNode(node: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      if (!isTrueMaster) return
    }
    val pool = ConnectionHelper.getNodePool(node)
    val businesses = new java.util.ArrayList[String]
    try {
      val results = DbUtils.executeQuery(pool, "select * from assigns")
      results.foreach(row => businesses.add(row.get(0)))
      log.info(s"add node ${node}, current responsible businesses is:${businesses}")
      nodes += (node -> businesses)


    } catch {
      case e: Exception =>
        log.info(s"add node ${node} failed, get assigns info error!!!", e)
    } finally {
      DbUtils.closePool(pool)
    }
  }

  def addNodeAssign(node: String, getData: Array[Byte]): Unit = {
    ScadbConf.wrapperWithLock { () =>
      if (!isTrueMaster) return
      val assigns = new Gson().fromJson(new String(getData), new TypeToken[java.util.List[String]]() {
      }.getType()).asInstanceOf[java.util.List[String]];

      nodes.get(node) match {
        case None => nodes += (node -> new java.util.ArrayList[String](assigns))
        case Some(current) if (current.contains(assigns) && assigns.containsAll(current)) =>
        case _ => log.error("node assigns not matched with the zk , will change to match with the zk, please check!!!")
          nodes(node) = new java.util.ArrayList[String](assigns)
      }
    }
  }

  def uptNodeAssign(node: String, getData: Array[Byte]): Unit = {
    addNodeAssign(node, getData)
  }

  def assignOneBusiness(business: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      if (!isTrueMaster || nodes.isEmpty) return
      val (node, cnt) = nodes.foldLeft(("", Int.MaxValue)) { (kv_fold, kv) =>
        if (kv_fold._2 > kv._2.size) (kv._1, kv._2.size)
        else kv_fold
      }

      nodes.get(node).foreach(_.add(business))

      assign(node, business, true) match {
        case (true, _) =>
          log.error(s"assign ${business} to node:${node} success!")
          syncToZk(node)
        case (false, msg) =>
          log.error(s"assign ${business} to node:${node} error: ${msg}")
      }
    }
  }

  def unassignOneBusiness(business: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      if (!isTrueMaster) return
      nodes.foreach { kv =>
        if (kv._2.contains(business)) {
          kv._2.remove(business)
          val node = kv._1
          assign(node, business, false) match {
            case (true, _) =>
              log.error(s"unassign ${business} from node:${node} success!")
              syncToZk(node)
              true
            case (false, msg) =>
              log.error(s"unassign ${business} from node:${node} error: ${msg}")
              false
          }
        }
      }
    }
  }


  def rebalance(): Boolean = {
    if ( !isTrueMaster ) return false
    val businesses = new mutable.HashSet[String]()
    AdminConf.businesses.foreach(businesses += _._1)

    val businessSize = businesses.size
    val nodeSize = nodes.size
    if ( nodeSize == 0 ) return false
    val average = businessSize / nodeSize
    var averagePlus = businessSize - average * nodeSize
    val tobeAssigns = new mutable.HashSet[String]
    businesses.foreach(tobeAssigns += _)

    var averagePlusTemp = averagePlus

    val nodeChanged = new mutable.HashSet[String]
    nodes.foreach { kv =>
      val values = kv._2
      while ( values.size > ( average + (if ( averagePlusTemp > 0) 1 else 0 )) ) {
        val business = values.remove(0)
        Preconditions.checkArgument(tobeAssigns.contains(business), "removed business %s must contained in the tobeassign lists", business)
        assign(kv._1, business, false) match {
          case (true, _) =>
          case (false, msg) =>
            log.error(s"unassign ${business} from node:${kv._2} error: ${msg}")
            return false
        }
        nodeChanged += kv._1
      }
      values.foreach(tobeAssigns -= _ )
      if ( averagePlusTemp > 0 ) averagePlusTemp -= 1
    }

    nodes.foreach { kv =>
      val values = kv._2
      while ( values.size < ( average + (if ( averagePlus > 0) 1 else 0 )) ) {
        val business = tobeAssigns.head
        values.add(business)
        tobeAssigns.remove(business)

        assign(kv._1, business, true) match {
          case (true, _) =>
          case (false, msg) =>
            log.error(s"assign ${business} to node:${kv._2} error: ${msg}")
            return false
        }
        nodeChanged += kv._1
      }
      averagePlus -= 1
    }

    nodeChanged.foreach { node =>
      syncToZk(node)
    }
    true
  }

  private def assign(node: String, business: String, isAssign: Boolean): (Boolean, String) = {
    var success = false
    var msg: String = null
    val latch = new CountDownLatch(1)

    val assignString = if ( isAssign ) s"set assign='${business}'" else s"set unassign='${business}'"
    val pool = ConnectionHelper.getNodePool(node)

    try {
      DbUtils.executeUpdateSingleStatement(pool, assignString)
      success = true
      if ( isAssign )
        log.info(s"assign ${business} to ${node} success!")
      else
        log.info(s"unassign ${business} from ${node} success!")
    } catch {
      case e: Exception =>
        if (isAssign)
          log.info(s"assign ${business} to ${node} failed ${e.getMessage}!")
        else
          log.info(s"unassign ${business} from ${node} failed ${e.getMessage}!")
        msg = e.getMessage
    } finally  {
      DbUtils.closePool(pool)
    }
    (success, msg)
  }

  private def syncToZk(node: String): Unit = {
  }

  class DetectorTimerTask extends TimerTask {
    override def run(): Unit = {
      log.info("rebalancing ......")
      ScadbConf.wrapperWithLock { () =>
        try {
          AdminMaster.rebalance()
        } catch {
          case e: Throwable =>
            log.error("rebalance error!",e)
        }
      }
    }
  }

}
