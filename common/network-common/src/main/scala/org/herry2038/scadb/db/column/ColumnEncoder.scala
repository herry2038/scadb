
package org.herry2038.scadb.db.column

trait ColumnEncoder {

  def encode(value: Any): String = value.toString

}
