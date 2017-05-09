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

import com.google.gson.Gson
import org.apache.curator.utils.ZKPaths
import ScadbConf.Business
import org.herry2038.scadb.conf.business.ScadbConfBusiness
import org.herry2038.scadb.util.Log

import scala.collection.JavaConversions._
import scala.collection.mutable

class ScadbConfManager {
  val log = Log.get[ScadbConfManager]
  val businesses = mutable.HashMap[String, ScadbConfBusiness]()
  lazy val businessesPathCache = ScadbConf.pathCache(ScadbConf.businessesPath, new ScadbConfBusinessesListener(this))
  var listeners = mutable.ArrayBuffer[ScadbConfManagerListener]()

  def start() = {
    ScadbConf.wrapperWithLock { () =>
      for (d <- businessesPathCache.getCurrentData) {
        val business = ZKPaths.getNodeFromPath(d.getPath)
        log.info("add business: {}", business)
        addBusiness(business, new String(d.getData))
      }
    }
  }

  def registerListener(listener: ScadbConfManagerListener) = {
    ScadbConf.wrapperWithLock { () =>
      listeners += listener
    }
  }

  def unregisterListener(listener: ScadbConfManagerListener) = {
    ScadbConf.wrapperWithLock { () =>
      listeners -= listener
    }
  }

  def addBusiness(business: String, data: String) = {
    ScadbConf.wrapperWithLock { ()=>
      val busiData = new Gson().fromJson(data, classOf[Business])
      val busi = new ScadbConfBusiness(business)
      listeners.map{ listener =>
        listener.addBusiness(business, busiData)
        if ( listener.isInstanceOf[ScadbConfListener] ) {
          busi.registerListener(listener.asInstanceOf[ScadbConfListener])
        }
      }
      businesses += (business->busi)
      busi.start()
    }
  }

  def delBusiness(business: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      businesses.remove(business).map { b =>
        log.info(String.format("remove business: %s %s success!", business, b))
        b.stop()
      }
    }
    listeners.map(_.delBusiness(business))
  }

  def updateBusiness(business: String, data: String): Unit = {
    /* 不处理变更，在ScadbBusiness中处理去
    val busiData = new Gson().fromJson(data, classOf[Business])
    ScadbConf.wrapperWithLock { () =>
      val busi = new ScadbConfBusiness(business)
      businesses(business) = busi
    }
    listeners.map(_.updateBusiness(business, busiData))
    */
  }
}
