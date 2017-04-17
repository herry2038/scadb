
package org.herry2038.scadb.db.pool

/**
 *
 * Raised when a pool has reached it's limit of available objects.
 *
 * @param message
 */

class PoolExhaustedException( message : String ) extends IllegalStateException( message )
