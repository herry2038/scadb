package org.herry2038.scadb.utils.gtid

/**
 * Created by Administrator on 2017/6/29.
 */
class Position(val file: String, val pos: Int) {
  def compare(o: Position): Int = {
    if ( file > o.file ) {
      1
    } else if ( file < o.file ) {
      -1
    } else {
      if  ( pos > o.pos )
        1
      else if ( pos < o.pos)
        -1
      else
        0
    }

  }
}
