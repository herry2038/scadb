
package org.herry2038.scadb.db.exceptions

class NegativeMessageSizeException( code : Byte, size : Int )
  extends DatabaseException( "Message of type %d had negative size %s".format(code, size) )