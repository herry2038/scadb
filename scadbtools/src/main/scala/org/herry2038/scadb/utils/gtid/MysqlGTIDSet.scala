package org.herry2038.scadb.utils.gtid

import java.util
import java.util.Collections
import com.google.common.base.Strings
import org.apache.commons.lang3.StringUtils

import scala.util.control.Breaks._
import scala.collection.JavaConversions._

/**
 * Created by Administrator on 2017/7/13.
 */
class MysqlGTIDSet(val sets: java.util.Map[String, MysqlGTIDSet.UUIDSet]) extends GTIDSet {
  def this() = this(new java.util.HashMap[String, MysqlGTIDSet.UUIDSet]())


  override def encode(): Array[Byte] = null

  override def equal(o: GTIDSet): Boolean = {
    if ( !o.isInstanceOf[MysqlGTIDSet]) return false
    val sub = o.asInstanceOf[MysqlGTIDSet]

    if ( sets.size != sub.sets.size ) return false

    sets.foreach { kv =>
      val oUuidSet = sub.sets.get(kv._1)
      if ( oUuidSet == null) return false
      if ( !oUuidSet.equals(kv._2)) return false
    }
    return true
  }

  override def contain(o: GTIDSet): Boolean = {
    if ( !o.isInstanceOf[MysqlGTIDSet]) return false
    val sub = o.asInstanceOf[MysqlGTIDSet]

    sub.sets.foreach { kv =>
      val uuidset = sets.get(kv._1)
      if ( uuidset == null ) return false

      if ( !uuidset.contain(kv._2)) return false
    }
    true
  }

  override def toString(): String = StringUtils.join(sets.entrySet(), ",")


  def addSet(set: MysqlGTIDSet.UUIDSet): Unit = {
    if ( set == null ) return

    val o = sets.get(set.getSID())
    if ( o != null ) {
      o.addInterval(set.getIntervals)
    } else {
      sets.put(set.getSID(), set)
    }
  }
}


object MysqlGTIDSet {
  class Interval (val start: Long, val stop: Long) extends Comparable[Interval]{
    override def toString(): String = {
      if ( stop == start + 1 ) {
        start.toString
      } else {
        s"${start}-${stop-1}"
      }
    }

    override def compareTo(t: Interval): Int = {
      if ( start < t.start ) -1
      else if ( start > t.start ) 1
      else
        (stop - t.stop).toInt
    }

  }

  class IntervalSlice extends java.util.ArrayList[Interval] {
    def swap(i: Int, j: Int): Unit = {
      val (v1, v2) = ( this.get(i), this.get(j) )
      this.set(i, v2)
      this.set(j, v1)
    }

    def sort(): Unit = {
      util.Collections.sort(this)
    }

    def normalize(): Unit = {
      if ( isEmpty ) return

      sort
//      val n = new IntervalSlice
//      n.add(get(0))
      var pos = 0
      while ( pos < size - 1 ) {
        val current = get(pos)
        val next = get(pos + 1 )
        if (next.start > next.stop) {
          pos += 1
        } else {
          set(pos, new Interval(current.start, next.stop))
        }
      }
    }

    def contain(sub: IntervalSlice): Boolean = {
      var j = 0
      for ( i <- 0 until sub.size ) {
        breakable {
          while (j < size()) {
            if (sub.get(i).start <= get(j).stop) {
              break
            }
            j += 1
          }
        }
        if ( j == size ) {
          return false
        }

        if ( sub.get(i).start < get(j).start || sub.get(i).stop > get(j).stop )
          return false ;
      }

      return true
    }

    override def equals(obj: Any): Boolean = {
      val o = obj.asInstanceOf[IntervalSlice]
      if ( size != o.size ) return false

      for ( i <- 0 until size ) {
        if ( get(i).start != o.get(i).start || get(i).stop != o.get(i).stop )
          return false
      }
      true
    }

    def compare(o: IntervalSlice): Int = {
      if ( equals(o) )
        0
      else if (contain(o))
        1
      else
        -1
    }
  }

  // Refer http://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
  class UUIDSet(SID_P: String, intervals_P: IntervalSlice ) {
    private val SID: String = SID
    private val intervals: IntervalSlice = intervals_P

    def this(SID_P: String) = this(SID_P, new IntervalSlice)


    def getIntervals = intervals
    def getSID() = SID

    def contain(sub: UUIDSet): Boolean = {
      if ( SID != sub.getSID() ) return false
      return intervals.contain(sub.getIntervals)
    }

    override def toString(): String = {
      val buffer = new StringBuilder
      buffer.append(SID)
      intervals.foreach(buffer.append(_))
      buffer.toString
    }

    def addInterval(slice: IntervalSlice): Unit = {
      slice.foreach(intervals.add(_))
      intervals.normalize()
    }
  }


  def parseInterval(str: String): Interval = {
    val strs = str.split(":")
    var start : Long = 0
    var stop : Long = 0
    strs.length match {
      case 1 =>
        start = strs(0).toLong
        stop = start + 1
      case 2=>
        start = strs(0).toLong
        stop = str(1).toLong + 1
      case _ => new RuntimeException(s"interval string invalid:${str}")
    }
    new Interval(start, stop)
  }

  def parseUUIDSet(str: String) : UUIDSet = {
    val sp = str.trim.split(":")
    if (sp.length < 2) {
      throw new RuntimeException("invalid GTID format, must be UUID:interval[:interval]")
    }

    val sid = sp(0)

    val intervalSet = new IntervalSlice
    for (i <- 1 until sp.length) {
      val in = parseInterval(sp(i))
      intervalSet.add(in)
    }
    new UUIDSet(sid, intervalSet)
  }


  def parseMysqlGTIDSet(str: String): GTIDSet = {
    val mysqlGtidSet = new MysqlGTIDSet()
    val sp = str.split(",")

    // Handle redundant same uuid
    sp.foreach { s =>
      val set = parseUUIDSet(s)
      mysqlGtidSet.addSet(set)
    }
    mysqlGtidSet
  }


}
