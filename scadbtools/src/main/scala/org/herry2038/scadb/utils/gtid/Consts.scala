package org.herry2038.scadb.utils.gtid

/**
 * Created by Administrator on 2017/6/28.
 */
object Consts {
  final val IO_THREAD = "IO_THREAD"
  final val SQL_THREAD = "SQL_THREAD"

  final val GTIDModeOn = "ON"
  final val GTIDModeOff = "OFF"

  class User(val user: String, val password: String)
}
