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
package org.herry2038.scadb.scadb.server

import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelOption
import io.netty.util.CharsetUtil
import java.util.concurrent.{TimeUnit, Executors}
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.mysql.server.{MySQLServerDelegateCreator, MySQLServerHandlerDelegate, MySQLServerConnectionHandler}
import org.herry2038.scadb.scadb.conf.{ScadbConfig, ScadbMySQLPools}
import org.herry2038.scadb.scadb.handler.ddl.DdlJobsListener
import org.herry2038.scadb.scadb.server.processor.{Statistics, ScadbUuid}
import org.herry2038.scadb.util.{Logging}


object ScadbServer extends Logging{


  def main(args: Array[String]): Unit = {
		ScadbConfig.load()
		ScadbUuid.getRunnerId
		val bossGroup = new NioEventLoopGroup()
		val ioGroup = new NioEventLoopGroup(ScadbConfig.conf.executeThreads)
		try {
			if (ScadbConfig.conf.port > 0) {
				val b = new ServerBootstrap()

				b.group(bossGroup, ioGroup).channel(classOf[NioServerSocketChannel])
				b.childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
				b.childOption[java.lang.Boolean](ChannelOption.SO_REUSEADDR, true)
				b.childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

				MySQLServerConnectionHandler.bind(b, CharsetUtil.UTF_8, new MySQLServerDelegateCreator() {
					def createDelegate(): MySQLServerHandlerDelegate = {
						new MySQLServerDelegateScadb
					}
				})
				info(s"start scadb at port ${ScadbConfig.conf.port}")

				val f = b.bind(ScadbConfig.conf.port).sync

				val timerExecutorService = Executors.newScheduledThreadPool(2)
				timerExecutorService.scheduleWithFixedDelay(new Runnable {
					override def run(): Unit = {
						ScadbMySQLPools.checkIdleConnection()
					}
				}, ScadbConfig.conf.idleCheckPeriod, ScadbConfig.conf.idleCheckPeriod, TimeUnit.MILLISECONDS)
				timerExecutorService.scheduleWithFixedDelay(new Runnable {
					override def run(): Unit = {
						Statistics.record
						DdlJobsListener.handleTimeoutJobs()
					}
				}, 10000, 10000, TimeUnit.MILLISECONDS)
				f.channel().closeFuture().sync()
			}
		} catch {
			case e: Exception=>
				e.printStackTrace()
		} finally {
			bossGroup.shutdownGracefully()
			ioGroup.shutdownGracefully()
			ScadbConf.stop
			System.exit(-1)
		}
	}
}
