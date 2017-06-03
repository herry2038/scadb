package org.herry2038.scadb.conf.common

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.utils.ZKPaths
import org.herry2038.scadb.util.Logging

/**
 * Created by Administrator on 2017/5/31.
 */
class ScadbConfPathListenerWithSubPath[Sub <: SubObject, T <: PathObject[Sub]](val t: T) extends PathChildrenCacheListener with Logging {
  override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
    event.getType match {
      case PathChildrenCacheEvent.Type.CHILD_ADDED =>
        val key = ZKPaths.getNodeFromPath(event.getData.getPath)
        info(s"path child added event: ${event.getData.getPath}")
        t.addKey(event.getData.getPath, key)
      case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
        info(s"path child updated event: ${event.getData.getPath}")
        val key = ZKPaths.getNodeFromPath(event.getData.getPath)
        t.uptKey(event.getData.getPath, key)
      case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
        info(s"path child removed event: ${event.getData.getPath}")
        val key = ZKPaths.getNodeFromPath(event.getData.getPath)
        t.delKey(event.getData.getPath, key)
      case a: Any =>
        info(s"unhandled path node msg: ${a}")
    }
  }
}
