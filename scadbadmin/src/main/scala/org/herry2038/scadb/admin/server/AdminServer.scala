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
package org.herry2038.scadb.admin.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelOption
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.CharsetUtil
import org.herry2038.scadb.mysql.server.{MySQLServerHandlerDelegate, MySQLServerDelegateCreator, MySQLServerConnectionHandler}
import org.herry2038.scadb.util.Logging


object AdminServer extends Logging {
  def main(args: Array[String]) {
    AdminConf.load
    val b = new ServerBootstrap()
    b.group(new NioEventLoopGroup(1)).channel(classOf[NioServerSocketChannel])
    b.childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY,true)
    b.childOption[java.lang.Boolean](ChannelOption.SO_REUSEADDR,true)

    MySQLServerConnectionHandler.bind(b, CharsetUtil.UTF_8,new MySQLServerDelegateCreator() {
      def createDelegate(): MySQLServerHandlerDelegate = {
        new MySQLServerDelegateScadbAdmin
      }
    })
    info(s"start distdbadmin:${AdminConf.node} success!")
    b.bind(AdminConf.port).sync
  }
}

