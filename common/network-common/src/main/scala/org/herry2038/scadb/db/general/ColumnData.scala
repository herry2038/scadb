
package org.herry2038.scadb.db.general

trait ColumnData {

  def name : String
  def dataType : Int
  def dataTypeSize : Long

}