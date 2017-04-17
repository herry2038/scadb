
package org.herry2038.scadb.db.exceptions

class ParserNotAvailableException(t: Byte)
  extends DatabaseException("There is no parser available for message type '%s' (%s)".format(t, Integer.toHexString(t)))