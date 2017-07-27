package org.herry2038.scadb.utils.gtid

/**
 * Created by Administrator on 2017/6/29.
 */
class MySQLFailoverHandler extends FailoverHandler{
  private final val changeMasterToWithAuto = """CHANGE MASTER TO
    |MASTER_HOST = "%s", MASTER_PORT = %s,
    |MASTER_USER = "%s", MASTER_PASSWORD = "%s",
    |MASTER_AUTO_POSITION = 1"""


  override def promote(s: Server): Unit = {
    waitRelayLogDone(s)
    s.stopSlave()
  }

  override def waitRelayLogDone(s: Server): Unit = {
    s.stopSlaveIOThread()
    val result = s.slaveStatus()
    if ( result.result.isEmpty)
      throw new RuntimeException("slave stauts is null")

    val retrived = result.col(0, "Retrieved_Gtid_Set")

    waitUntilAfterGTIDs(s, retrived)
  }

  override def betterThan(a: Server, b: Server): Boolean = {
    val aPos = a.fetchSlaveExecutePos()
    val bPos = b.fetchSlaveExecutePos()
    aPos.compare(bPos) > 0
  }

  override def findBestSlaves(slaves: Array[Server]): Array[Server] = {
    // MHA use Relay_Master_Log_File and Exec_Master_Log_Pos to determine witch is the best slave

    var bestSlaves: Array[Server] = null

    var lastSlave: Server = null
    var bestPos: Position = null
    slaves.foreach { slave =>
      val pos = slave.fetchSlaveExecutePos()

      if ( lastSlave == null ) {
        lastSlave = slave
        bestSlaves = Array(slave)
        bestPos = pos
      } else {
        bestPos.compare(pos) match {
          case 1 =>
          case -1 =>
            lastSlave = slave
            bestSlaves = Array(slave)
          case 0 =>
            bestSlaves = Array.concat(bestSlaves, Array(slave))
        }
      }
    }
    bestSlaves
  }

  override def checkGTIDMode(slaves: Array[Server]): (Boolean, String) = {
    slaves.foreach{slave=>
      if ( !slave.mysqlGtidMode() ) {
        ( false, s"${slave.ip}:${slave.port} use not GTID mode")
      }
    }
    (true, null)
  }

  override def waitCatchMaster(s: Server, m: Server): Unit = {
    val r = m.masterStatus()
    val masterGTIDSet = r.col(0, "Executed_Gtid_Set")

    waitUntilAfterGTIDs(s, masterGTIDSet)
  }

  override def changeMasterTo(s: Server, m: Server): Unit = {
    waitRelayLogDone(s)

    s.stopSlave()
    s.resetSlave()
    s.executeUpdate(String.format(changeMasterToWithAuto, m.ip, m.port.toString, m.replUser.user, m.replUser.password))
    s.startSlave()
  }


  private def waitUntilAfterGTIDs(s: Server, gtids: String): Unit = {
    s.executeQuery(s"SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('${gtids}')")
  }

}
