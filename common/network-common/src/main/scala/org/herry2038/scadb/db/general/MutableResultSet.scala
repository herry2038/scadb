
package org.herry2038.scadb.db.general

import org.herry2038.scadb.db.{RowData, ResultSet}
import org.herry2038.scadb.db.util.Log

import collection.mutable.ArrayBuffer


object MutableResultSet {
  val log = Log.get[MutableResultSet[Nothing]]
}

class MutableResultSet[T <: ColumnData](
                        val columnTypes: IndexedSeq[T]) extends ResultSet {

  private val rows = new ArrayBuffer[RowData]()
  private val columnMapping: Map[String, Int] = this.columnTypes.indices.map(
    index =>
      ( this.columnTypes(index).name, index ) ).toMap
    

  val columnNames : IndexedSeq[String] = this.columnTypes.map(c => c.name)

  override def length: Int = this.rows.length

  override def apply(idx: Int): RowData = this.rows(idx)

  def addRow( row : Seq[Any] ) {
    val realRow = new ArrayRowData( columnTypes.size, this.rows.size, this.columnMapping )
    var x = 0
    while ( x < row.size ) {
      realRow(x) = row(x)
      x += 1
    }
    this.rows += realRow
  }

}