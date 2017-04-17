
package org.herry2038.scadb.db

/**
 *
 * Represents a row from a database, allows clients to access rows by column number or column name.
 *
 */

trait RowData extends IndexedSeq[Any] {

  /**
   *
   * Returns a column value by it's position in the originating query.
   *
   * @param columnNumber
   * @return
   */

  def apply( columnNumber : Int ) : Any

  /**
   *
   * Returns a column value by it's name in the originating query.
   *
   * @param columnName
   * @return
   */

  def apply( columnName : String ) : Any

  /**
   *
   * Number of this row in the query results. Counts start at 0.
   *
   * @return
   */

  def rowNumber : Int

}
