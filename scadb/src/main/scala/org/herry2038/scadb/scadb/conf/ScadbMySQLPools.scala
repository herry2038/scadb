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
package org.herry2038.scadb.scadb.conf

import java.nio.charset.Charset
import java.util
import java.util.concurrent.ConcurrentHashMap

import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.cluster.ClusterModel.{MasterModel, MySQLStatus}
import org.herry2038.scadb.conf.cluster.{ClusterConfListener}
import org.herry2038.scadb.db.pool._
import org.herry2038.scadb.mysql.MySQLConnection
import org.herry2038.scadb.util.Log
import scala.collection.JavaConversions._
import org.herry2038.scadb.db.{Connection, Configuration}
import org.herry2038.scadb.mysql.pool.MySQLConnectionFactory


object ScadbMySQLPools extends ClusterConfListener{
	class Anonymous

	val log = Log.get[Anonymous]


	class ProxyInfo {
		var master: String = _

		val pools = new ConcurrentHashMap[String, Connection]

		var thisGroups = new util.ArrayList[String]()
		var currentSeeds: Int = 0

		def getPool(isRead: Boolean) = {
			val choosedPoolName = if ( isRead ) {
				thisGroups.synchronized {
					val size = thisGroups.size()
					if (size == 0) {
						master
					} else {
						val pos = currentSeeds % size
						currentSeeds = currentSeeds + 1
						thisGroups.get(pos)
					}
				}
			} else {
				master
			}
			if (choosedPoolName == null) null else pools.get(choosedPoolName)
		}

		def uptMasterModel(masterModel: MasterModel) = {
			this.master = masterModel.currentMaster
		}

		def createPool(poolConf: PoolConfiguration, conf: Configuration) = {
			new MultiThreadedConnectionPool[MySQLConnection](conf.host + ":" + conf.port, new MySQLConnectionFactory(conf), poolConf)
		}


		def addInstance(instance: String, status: MySQLStatus) = {
			val ipAndPort = instance.split(":")
			val conf = new Configuration(ScadbConf.backuser, ipAndPort(0), ipAndPort(1).toInt, Some(ScadbConf.backpass),
				Some(ScadbConfig.conf.busi.dbPrefix),
				Charset.forName(ScadbConfig.conf.busi.charset), true, false, true)

			val poolConf = new PoolConfiguration(ScadbConfig.conf.busi.minConnections, ScadbConfig.conf.busi.maxConnections,ScadbConfig.conf.idleMaxWaitTime, ScadbConfig.conf.maxQueueSize, ScadbConfig.conf.idleCheckPeriod)
			val pool = createPool(poolConf, conf)
			pools.putIfAbsent(instance, pool)

			if ( ScadbConfig.conf.zoneid == status.zoneid ) {
				thisGroups.add(instance)
			}
		}

		def uptInstance(instance: String, status: MySQLStatus): Unit = {

		}

		def delInstance(instance: String) : Unit = {
			val ipAndPort = instance.split(":")
			Option(pools.get(instance)).map(_.disconnect)
			pools.remove(instance)
			thisGroups.remove(instance)
		}

		def idleCheck(idleCheckConnectionsInPool: Int) = {
			for ( connection <- pools.values ) {
				connection.asInstanceOf[AsyncObjectPool[MySQLConnection]].testObjects
			}
		}
	}

	val instances = new ConcurrentHashMap[String, ProxyInfo]()
	

	def getPool(instance: String, isRead: Boolean): Option[Connection] = {
		Option(instances.get(instance)).map { _.getPool(isRead) }
	}

	override def delInstance(proxy: String, instance: String): Unit = {
		log.info(String.format("del proxy [%s] , instance:%s", proxy, instance))
		Option(instances.get(proxy)).map(_.delInstance(instance))
	}

	override def uptInstance(proxy: String, instance: String, status: MySQLStatus): Unit = {
		log.info(String.format("upt instance [%s] , instance:%s, status:%s", proxy, instance, status))
		Option(instances.get(proxy)).map(_.uptInstance(instance,status))
	}

	override def addInstance(proxy: String, instance: String, status: MySQLStatus): Unit = {
		log.info(String.format("add instance [%s] , instance:%s, status:%s", proxy, instance, status))
		Option(instances.get(proxy)).map(_.addInstance(instance,status))
	}

	override def uptMasterModel(proxy: String, masterModel: MasterModel): Unit = {
		log.info(String.format("upt proxy [%s] , masterModel:%s", proxy, masterModel))
		Option(instances.get(proxy)).map(_.uptMasterModel(masterModel)).getOrElse {
			val p = new ProxyInfo()
			p.uptMasterModel(masterModel)
			instances.put(proxy, p)
		}
	}

	def checkIdleConnection(): Unit = {
		for ( proxyInfo <- instances.values() ) {
			proxyInfo.idleCheck(ScadbConfig.conf.idleCheckConnectionsInPool)
		}
	}
}
