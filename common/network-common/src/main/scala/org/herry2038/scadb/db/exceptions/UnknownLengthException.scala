
package org.herry2038.scadb.db.exceptions

class UnknownLengthException ( length : Int )
  extends DatabaseException( "Can't handle the length %d".format(length) )