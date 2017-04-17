package org.herry2038.scadb.db.util

import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}


object NettyUtils {

  InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
  lazy val DefaultEventLoopGroup = new NioEventLoopGroup(0, DaemonThreadsFactory("db-async-netty"))

}