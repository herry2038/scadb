
package org.herry2038.scadb.db.exceptions

class DatabaseException(message: String, cause : Throwable) extends RuntimeException(message) {

  def this( message : String ) = this(message, null)

}