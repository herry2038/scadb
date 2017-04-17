
package org.herry2038.scadb.db.exceptions

class UnsupportedAuthenticationMethodException(val authenticationType: String)
  extends DatabaseException("Unknown authentication method -> '%s'".format(authenticationType)) {

  def this( authType : Int ) {
    this(authType.toString)
  }

}