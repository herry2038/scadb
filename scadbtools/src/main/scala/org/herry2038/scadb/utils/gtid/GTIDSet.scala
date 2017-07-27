package org.herry2038.scadb.utils.gtid

/**
 * Created by Administrator on 2017/7/13.
 */
trait GTIDSet {
  def toString(): String
  def encode(): Array[Byte]
  def equal(o: GTIDSet): Boolean
  def contain(o: GTIDSet): Boolean
}

object GTIDSet {
  final val mysqlType = 0
  final val mariadbType = 1
  def parse(t: Int, s: String): GTIDSet = {
    if ( t == mysqlType )
      return MysqlGTIDSet.parseMysqlGTIDSet(s)
    else if ( t == mariadbType )
      return MariadbGTIDSet.parseMariadbGTIDSet(s)
    else
      throw new RuntimeException(s"unknown type:${t}")
  }
}
