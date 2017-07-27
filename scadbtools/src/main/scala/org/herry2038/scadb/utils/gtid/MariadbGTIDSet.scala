package org.herry2038.scadb.utils.gtid

/**
 * Created by Administrator on 2017/7/26.
 */
class MariadbGTIDSet(val domainId: Int, val serverId: Int, val sequenceNumber: Long) extends GTIDSet {
  override def encode(): Array[Byte] = null

  override def equal(o: GTIDSet): Boolean = {
    if ( !o.isInstanceOf[MariadbGTIDSet] ) return false
    val other = o.asInstanceOf[MariadbGTIDSet]

    return domainId == other.domainId && serverId == other.serverId && sequenceNumber == other.sequenceNumber
  }

  override def contain(o: GTIDSet): Boolean = {
    if ( !o.isInstanceOf[MariadbGTIDSet] ) return false

    val other = o.asInstanceOf[MariadbGTIDSet]
    return domainId == other.domainId && serverId == other.serverId && sequenceNumber >= other.sequenceNumber
  }
}

object MariadbGTIDSet {
  // We don't support multi source replication, so the mariadb gtid set may have only domain-server-sequence
  def parseMariadbGTIDSet(str: String): GTIDSet = {
    if ( str.isEmpty ) return new MariadbGTIDSet(0,0,0)

    val sp = str.split("-")
    if ( sp.length != 3 )
      throw new RuntimeException(s"invalid Mariadb GTID ${str}, must be domain-server-sequence")

    new MariadbGTIDSet(sp(0).toInt, sp(1).toInt, sp(2).toLong)
  }
}