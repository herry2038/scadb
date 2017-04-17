
package org.herry2038.scadb.db.exceptions

class ConnectionStillRunningQueryException( connectionCount : Long, caughtRace : Boolean)
  extends DatabaseException ( "[%s] - There is a query still being run here - race -> %s".format(
    connectionCount,
    caughtRace
  ))
