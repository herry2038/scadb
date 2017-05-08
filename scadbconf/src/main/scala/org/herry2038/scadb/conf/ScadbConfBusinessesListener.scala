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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.utils.ZKPaths
import org.herry2038.scadb.util.Log

/**
 * Created by Administrator on 2016/2/22.
 */
class ScadbConfBusinessesListener(manager: ScadbConfManager) extends PathChildrenCacheListener{
  val log = Log.get[ScadbConfBusinessesListener]
  override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
    event.getType match {
      case PathChildrenCacheEvent.Type.CHILD_ADDED =>
        val business = ZKPaths.getNodeFromPath(event.getData.getPath)
        manager.addBusiness(business,new String(event.getData.getData))
      case PathChildrenCacheEvent.Type.CHILD_UPDATED=>
        val business = ZKPaths.getNodeFromPath(event.getData.getPath)
        manager.updateBusiness(business, new String(event.getData.getData))
      case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
        val business = ZKPaths.getNodeFromPath(event.getData.getPath)
        manager.delBusiness(business)
      case a: Any =>
        log.info("unhandled path node msg:{}", a)
    }
  }
}
