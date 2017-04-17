
package org.herry2038.scadb.db.exceptions

import io.netty.channel.ChannelFuture

class CanceledChannelFutureException( val channelFuture : ChannelFuture )
  extends IllegalStateException ( "This channel future was canceled -> %s".format(channelFuture) )
