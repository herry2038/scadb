package org.herry2038.scadb.scadb.handler.executeplan

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/8/26.
 */
object MergeSorter {

  trait Compare[A] {
    def compare(l: A, r: A): Long
  }
  class MergeSortor[A](val compare: Compare[A]) {
    private val results = new ArrayBuffer[IndexedSeq[A]]
    private val currentPoses = new ArrayBuffer[Int]
    private val sortedResultIds = new ArrayBuffer[Int]

    def addSorter(indexedSeq: IndexedSeq[A]): Unit = {
      if ( !indexedSeq.isEmpty ) {
        results += indexedSeq
        currentPoses += 0
        insert(results.size - 1, 0)
      }
    }

    def next(): Option[A] = {
      if ( ! sortedResultIds.isEmpty ) {
        val rsId = sortedResultIds.remove(0)

        val row = results(rsId)(currentPoses(rsId))
        currentPoses(rsId) += 1

        if ( currentPoses(rsId) < results(rsId).size ) {
          insert(rsId, currentPoses(rsId))
        }
        Some(row)
      } else
        None
    }

    def insert(rsId: Int, offset: Int): Unit = {
      var min = 0
      var max = sortedResultIds.size - 1

      var mid = 0
      while ( min <= max ) {
        mid = ( min + max ) / 2

        val midRsId = sortedResultIds(mid)
        val compareResult = compare.compare(results(rsId)(offset), results(midRsId)(currentPoses(midRsId)))

        if ( compareResult > 0 )
          min = mid + 1
        else
          max = mid - 1
      }

      sortedResultIds.insert(max + 1, rsId)
    }

  }
}
