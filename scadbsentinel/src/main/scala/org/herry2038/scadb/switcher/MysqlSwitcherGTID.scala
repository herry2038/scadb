package org.herry2038.scadb.switcher

import com.google.gson.Gson
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.cluster.ClusterConf
import org.herry2038.scadb.conf.cluster.ClusterModel.{MasterModel, MySQLStatus}
import org.herry2038.scadb.util.Logging
import org.herry2038.scadb.utils.gtid.{Consts, Server, MySQLFailoverHandler}

/**
 * Created by Administrator on 2017/7/27.
 */
class MysqlSwitcherGTID extends MysqlSwitcher with Logging {
  val failoverHandler = new MySQLFailoverHandler
  val user = new Consts.User(ScadbConf.backuser, ScadbConf.backpass)

  override def isBetterThan(host: String, status: MySQLStatus, compareToHost: String, compareTo: MySQLStatus): Boolean = {
    val sp1 = host.split(":")
    val sp2 = compareToHost.split(":")

    val currentServer = new Server(sp1(0), sp1(1).toInt, user, user)
    val compareServer = new Server(sp2(0), sp2(1).toInt, user, user)

    failoverHandler.betterThan(currentServer, compareServer)
  }

  override def switch(cluster: SentinelAutoSwitch, destination: String): (Boolean, MasterModel) = {
    val parts = cluster.cluster.split("\\.")
    val path = ClusterConf.path(parts(0), parts(1))
    val model = new MasterModel("", cluster.clusterModel.switchMode)
    // 1. 把原来的master设置为STOP状态
    ScadbConf.client.setData().forPath(path, new Gson().toJson(model).getBytes)
    val sp = destination.split(":")
    val destinationServer = new Server(sp(0), sp(1).toInt, user, user)
    try {
      // 提升
      failoverHandler.promote(destinationServer)
      // 把所有的slave 指向它
      cluster.validHosts().foreach{ host =>
        if ( host != destination ) {
          val spServer = host.split(":")
          val server = new Server(spServer(0), spServer(1).toInt, user, user)
          failoverHandler.changeMasterTo(server, destinationServer)
        }
      }

      val model = new MasterModel(destination, cluster.clusterModel.switchMode)
      ScadbConf.client.setData().forPath(path, new Gson().toJson(model).getBytes)
      (true, model)
    } catch {
      case ex: Exception =>
        error(s"sync to zk url: ${path} error!", ex)
        (false, null)
    }
  }
}
