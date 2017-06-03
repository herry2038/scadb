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

package org.herry2038.scadb.switcher

import java.util.concurrent.ConcurrentHashMap

import org.herry2038.scadb.conf.cluster.ClusterModel.{MasterModel, MySQLStatus}
import org.herry2038.scadb.conf.cluster.{Role, Status, SwitchModel}
import org.herry2038.scadb.sentinel.SentinelAutoswitchService
import org.herry2038.scadb.util.Logging

import scala.collection.JavaConversions._

class SentinelAutoSwitch(val cluster: String) extends Logging {
  var clusterModel: MasterModel = null
  val instances = new ConcurrentHashMap[String, MySQLStatus]

  private var inited = false
  def delInstance(instance: String): Unit = {
    instances.remove(instance)

    if ( inited )
      checkSwitch
  }

  def uptInstance(instance: String, status: MySQLStatus): Unit = {
    instances.put(instance, status)
    if ( inited )
      checkSwitch
  }

  def addInstance(instance: String, status: MySQLStatus): Unit = {
    instances.put(instance, status)
    if ( !inited && clusterModel != null && instance == clusterModel.currentMaster )
      inited = true

    if ( inited ) {
      checkSwitch
    }
  }

  def uptMasterModel(clusterModel: MasterModel): Unit = {
    this.clusterModel = clusterModel
  }

  def checkMasterIsDown(): (Boolean, String) = {
    var masterIsDown = false
    var bestCandidate: String = null
    var bestStatus: MySQLStatus = null
    var masterNotExists = true

    instances.foreach{ instance=>
      if ( clusterModel.currentMaster == instance._1 ) {
        masterNotExists = false
        masterIsDown = instance._2.status != Status.START
      } else if ( instance._2.role == Role.CANDIDATE ) {
        if ( instance._2.status == Status.START) {
          if ( bestCandidate == null || SentinelAutoswitchService.switcher.isBetterThan(instance._2, bestStatus)) {
            bestCandidate = instance._1
            bestStatus = instance._2
          }
        }
      }
    }
    (masterIsDown || masterNotExists, bestCandidate)
  }


  def checkSwitch(): Unit = {
    if ( !SentinelAutoswitchService.started || clusterModel.switchMode == SwitchModel.MANUAL ) return

    val (isDown, bestCandidate) = checkMasterIsDown

    if ( isDown  ) {
      info(s"autoswitch cluster: ${cluster} found ${clusterModel.currentMaster} is down, will switch to ${bestCandidate}")
      if ( bestCandidate != null ) {
        if (SentinelAutoswitchService.switcher.switch(this, bestCandidate)) {
          info(s"autoswitch cluster: ${cluster} , switch to ${bestCandidate} succeed!!!")
        } else {
          warn(s"autoswitch cluster: ${cluster} , switch to ${bestCandidate} failed!!!")
        }
      }
    }
  }
}
