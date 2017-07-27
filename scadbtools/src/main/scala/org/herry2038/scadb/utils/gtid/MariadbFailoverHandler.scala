package org.herry2038.scadb.utils.gtid

/**
 * Created by Administrator on 2017/7/13.
 */
class MariadbFailoverHandler extends FailoverHandler{

  private final val changeMasterToWithCurrentPos = """CHANGE MASTER TO
    |MASTER_HOST = "%s", MASTER_PORT = %s,
    |MASTER_USER = "%s", MASTER_PASSWORD = "%s",
    |MASTER_USE_GTID = current_pos"""

  override def promote(s: Server): Unit = {
    waitRelayLogDone(s)
    s.stopSlave()
  }

  override def waitRelayLogDone(s: Server): Unit = {
    s.stopSlaveIOThread()

    val r = s.slaveStatus()

    val fname = r.col(0, "Master_Log_File")
    val pos = r.col(0, "Read_Master_Log_Pos")

    s.masterPosWait(new Position(fname, pos.toInt), 0)
  }

  override def betterThan(a: Server, b: Server): Boolean = {
    val ars = a.executeQuery("SELECT @@gtid_current_pos")
    val astr = ars.col(0,0)
    val brs = b.executeQuery("SELECT @@gtid_current_pos")
    val bstr = brs.col(0,0)
    MariadbGTIDSet.parseMariadbGTIDSet(astr).asInstanceOf[MariadbGTIDSet].sequenceNumber >
      MariadbGTIDSet.parseMariadbGTIDSet(bstr).asInstanceOf[MariadbGTIDSet].sequenceNumber
  }

  override def findBestSlaves(slaves: Array[Server]): Array[Server] = {
    var bestSlaves: Array[Server] = null

    var lastSlave: Server = null
    var bestPos: Long = 0

    slaves.foreach { slave =>
      val rs = slave.executeQuery("SELECT @@gtid_current_pos")
      val str = rs.col(0, 0)

      val seq = MariadbGTIDSet.parseMariadbGTIDSet(str).asInstanceOf[MariadbGTIDSet].sequenceNumber
      if (lastSlave == null) {
        lastSlave = slave
        bestSlaves = Array(slave)
        bestPos = seq
      } else {
        if (bestPos == seq) {
          bestSlaves = Array.concat(bestSlaves, Array(slave))
        } else if (bestPos < seq) {
          lastSlave = slave
          bestSlaves = Array(slave)
        }
      }
    }
    bestSlaves
  }

  override def checkGTIDMode(slaves: Array[Server]): (Boolean, String) = (true, null)

  override def waitCatchMaster(s: Server, m: Server): Unit = {
    val result = m.executeQuery("SELECT @@gtid_binlog_pos")

    val pos = result.col(0, 0)
    waitUntilAfterGTID(s, pos)
  }


  private def waitUntilAfterGTID(s: Server, pos: String): Unit = {
    s.executeQuery(s"SELECT MASTER_GTID_WAIT('${pos}'")
  }

  override def changeMasterTo(s: Server, m: Server): Unit = ???
}
