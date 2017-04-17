
package org.herry2038.scadb.db.general

import org.herry2038.scadb.db.RowData
import scala.collection.mutable

class ArrayRowData( columnCount : Int, row : Int, val mapping : Map[String, Int] )
  extends RowData
{

  private val columns = new Array[Any](columnCount)

  /**
   *
   * Returns a column value by it's position in the originating query.
   *
   * @param columnNumber
   * @return
   */
  def apply(columnNumber: Int): Any = columns(columnNumber)

  /**
   *
   * Returns a column value by it's name in the originating query.
   *
   * @param columnName
   * @return
   */
  def apply(columnName: String): Any = columns( mapping(columnName) )

  /**
   *
   * Number of this row in the query results. Counts start at 0.
   *
   * @return
   */
  def rowNumber: Int = row

  /**
   *
   * Sets a value to a column in this collection.
   *
   * @param i
   * @param x
   */

  def update(i: Int, x: Any) = columns(i) = x

  def length: Int = columns.length

}
