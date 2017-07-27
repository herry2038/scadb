package org.herry2038.scadb.utils.gtid

/**
 * Created by Administrator on 2017/6/29.
 */
trait FailoverHandler {
  def promote(s: Server): Unit

  def betterThan(a: Server, b: Server): Boolean

  def changeMasterTo(s: Server, m: Server): Unit

  def waitRelayLogDone(s: Server): Unit

  def waitCatchMaster(s: Server, m: Server): Unit

  def findBestSlaves(slaves: Array[Server]): Array[Server]

  def checkGTIDMode(slaves: Array[Server]): (Boolean, String)

}
