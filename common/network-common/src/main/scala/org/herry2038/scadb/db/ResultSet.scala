

package org.herry2038.scadb.db

/**
 *
 * Represents the collection of rows that is returned from a statement inside a {@link QueryResult}. It's basically
 * a collection of Array[Any]. Mutating fields in this array will not affect the database in any way
 *
 */

trait ResultSet extends IndexedSeq[RowData] {

  /**
   *
   * The names of the columns returned by the statement.
   *
   * @return
   */

  def columnNames : IndexedSeq[String]

}