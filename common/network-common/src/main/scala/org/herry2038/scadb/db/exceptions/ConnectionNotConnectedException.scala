package org.herry2038.scadb.db.exceptions

import org.herry2038.scadb.db.Connection

class ConnectionNotConnectedException( val connection : Connection )
  extends DatabaseException( "The connection %s is not connected to the database".format(connection) )