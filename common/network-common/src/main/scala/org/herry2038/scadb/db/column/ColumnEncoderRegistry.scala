
package org.herry2038.scadb.db.column

trait ColumnEncoderRegistry {

  def encode( value : Any ) : String

  def kindOf( value : Any ) : Int

}
