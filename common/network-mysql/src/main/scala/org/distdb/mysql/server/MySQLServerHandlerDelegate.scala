package org.distdb.mysql.server

import java.net.InetSocketAddress

import com.github.mauricio.async.db.mysql.message.client.HandshakeResponseMessage
import io.netty.channel.ChannelHandlerContext
import com.github.mauricio.async.db.mysql.message.server.HandshakeMessage
import com.github.mauricio.async.db.mysql.decoder.HandshakeV10Decoder
import io.netty.util.CharsetUtil
import scala.util.Random
import com.github.mauricio.async.db.mysql.encoder.auth.AuthenticationMethod
import com.github.mauricio.async.db.util.Log
import java.util.Arrays
import com.github.mauricio.async.db.mysql.message.server.OkMessage
import com.github.mauricio.async.db.mysql.message.server.ErrorMessage

trait MySQLServerHandlerDelegate {
  import MySQLServerHandlerDelegate._
  var handler: MySQLServerConnectionHandler = null
  var scramble : Array[Byte] = new Array[Byte](HandshakeV10Decoder.SeedSize + HandshakeV10Decoder.SeedComplementSize)
  
  def setHandler(handler: MySQLServerConnectionHandler) {
    this.handler = handler
  }

  def ok(): Unit = {
    handler.write(okMessage)
  }

  def error(errorNo: Int, errorMsg: String): Unit = {
    handler.write(new ErrorMessage(errorNo, errorMsg))
  }

  def getPassword(userName: String): String
  def isInWhitelist(clientIp: String): Boolean

  def onHandshakeResponse( message : HandshakeResponseMessage ) {
    val userInfos = message.username.split("@")
    if ( userInfos.length > 1 ) {
      if ( userInfos(1) == "rws" ) {
        handler.isReadWriteSplit = true;
      } else {
        handler.write(new ErrorMessage(1045, s"Access denied for user '$message.username'@'"))
        return
      }
    }
    val password = getPassword(userInfos(0))
    if ( password == null ) {
      handler.write(new ErrorMessage(1045, s"Access denied for user '${message.username}'@'"))
      return
    }

    val md = java.security.MessageDigest.getInstance("SHA-1")
    val passHash = md.digest(password.getBytes)
    val passHash2 = md.digest(passHash)
    val buf = new Array[Byte](scramble.length + passHash2.length)
    System.arraycopy(scramble, 0, buf, 0, scramble.length)
    System.arraycopy(passHash2, 0, buf, scramble.length, passHash2.length)
    val tmpHash = md.digest(buf)
    val resultHash = xor(passHash, tmpHash)
    //val resultHash = md.digest(stageHash)
    val messagePassHash = message.scramble.getBytes(CharsetUtil.ISO_8859_1)
    val result = Arrays.equals(messagePassHash, resultHash)
    //val result = message.password.map(pass =>
    //  Arrays.equals(pass.getBytes("ISO_8859_1"),resultHash) )
    val outMsg =
    if ( !result ) {
      // write error msg back
      new ErrorMessage(1045, s"Access denied for user '${message.username}")
    } else {
      // write ok msg back
      handler.authenticated
      new OkMessage(0,0,0,0,null)
    }
    handler.write(outMsg)      
  }
  
  def onQuery( sql : String )  {
    handler.write(new OkMessage(0,0,0,0,null))
  }
  
  def onQuit() {
    handler.disconnect
  }
  
  def exceptionCaught( exception : Throwable ) {
    log.error("delegate get a exception:",exception)
    handler.disconnect
  }
  
  
  def connected( ctx : ChannelHandlerContext ) {
    val insocket = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
    val clientIP = insocket.getAddress().getHostAddress();
    if ( !isInWhitelist(clientIP) ) {
      ctx.close()
      return
    }
    getScrambleBuf(scramble)
    var capbilities = 0xF7FFL
        
    val handleShake = new HandshakeMessage(
        "5.6.21-distdb",
        0,
        scramble,
        capbilities.asInstanceOf[Int],
        83,
        2,
        AuthenticationMethod.Native
        )
    handler.write(handleShake)
  }
}

trait MySQLServerDelegateCreator {
  def createDelegate(): MySQLServerHandlerDelegate
}

object MySQLServerHandlerDelegate {  
  final val log = Log.getByName(s"[server-handler-delegate]")
  val scrableSeed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  val scrableSeedBytes = scrableSeed.getBytes()
  val random = new Random
  val okMessage = new OkMessage(0,0,0,0,null)
  
  def getScrambleBuf(seed:Array[Byte]) {        
    for ( i <- 0 until HandshakeV10Decoder.SeedSize + HandshakeV10Decoder.SeedComplementSize ) {
      seed(i) = scrableSeedBytes(random.nextInt(scrableSeedBytes.length))
    }    
  }
  
  
  def xor(input: Array[Byte], secret: Array[Byte]) :Array[Byte] = {
    val output = new Array[Byte](input.length);
    
    if (secret.length == 0) {
        throw new IllegalArgumentException("empty security key");
    }
    var spos = 0
    for (pos <- 0 until input.length) {
        output(pos) = (input(pos) ^ secret(spos)).asInstanceOf[Byte];
        spos += 1
        if (spos >= secret.length) {
            spos = 0;
        }
    }
    return output;
}
}

