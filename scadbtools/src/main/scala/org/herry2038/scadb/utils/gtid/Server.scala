package org.herry2038.scadb.utils.gtid

import org.herry2038.scadb.util.MySQLConnection
import org.herry2038.scadb.util.MySQLConnection.MySQLResult


/**
 * Created by Administrator on 2017/6/28.
 */
class Server(val ip: String, val port: Int, val user: Consts.User, val replUser: Consts.User) {
  private val mysqlUrl = s"jdbc:mysql://${ip}:${port}/?connectTimeout=60000&socketTimeout=60000&jdbcCompliantTruncation=false&useUnicode=true&characterEncoding=UTF-8"
  lazy private val conn: MySQLConnection = new MySQLConnection(mysqlUrl, user.user, user.password)


  def startSlave() : Unit = conn.executeUpdate("START SLAVE")

  def stopSlave(): Unit = conn.executeUpdate("STOP SLAVE")

  def stopSlaveIOThread(): Unit = conn.executeUpdate("STOP SLAVE IO_THREAD")

  def slaveStatus(): MySQLResult = conn.executeQuery("SHOW SLAVE STATUS")

  def masterStatus(): MySQLResult = conn.executeQuery("SHOW MASTER STATUS")

  def resetSlave() :Unit = conn.executeUpdate("RESET SLAVE")

  def resetSlaveALL(): Unit = conn.executeUpdate("RESET SLAVE ALL")

  def resetMaster(): Unit = conn.executeUpdate("RESET MASTER")

  def mysqlGtidMode(): Boolean = {
    val result = conn.executeQuery("SELECT @@gtid_mode")
    result.col(0, 0)  == Consts.GTIDModeOn
  }


  def setReadOnly(b: Boolean): Unit = {
    if ( b )
      conn.executeUpdate("SET GLOBAL read_only = ON")
    else
      conn.executeUpdate("SET GLOBAL read_only = OFF")
  }

  def lockTables(): Unit = conn.executeUpdate("FLUSH TABLES WITH READ LOCK")

  def unlockTables(): Unit = conn.executeUpdate("UNLOCK TABLES")

  /**
   * Get current binlog filename and position read from master
   */
  def fetchSlaveReadPos(): Position = {
    val result = slaveStatus()
    if ( result.result.isEmpty)
      throw new RuntimeException("slave stauts is null")
    val fname = result.col(0, "Master_Log_File")
    val pos = result.col(0, "Read_Master_Log_Pos")
    new Position(fname, pos.toInt)
  }

  // Get current executed binlog filename and position from master

  def fetchSlaveExecutePos(): Position = {
    val result = slaveStatus()
    if ( result.result.isEmpty)
      throw new RuntimeException("slave stauts is null")

    val fname = result.col(0, "Relay_Master_Log_File")
    val pos = result.col(0, "Exec_Master_Log_Pos")
    new Position(fname, pos.toInt)
  }


  def masterPosWait(pos: Position, timeout: Int): Unit = {
    conn.executeQuery("SELECT MASTER_POS_WAIT('%s', %d. %d".format(pos.file, pos.pos, timeout))
  }

  def executeQuery(sql: String): MySQLResult = conn.executeQuery(sql)

  def executeUpdate(sql: String): Int  = conn.executeUpdate(sql)


}
