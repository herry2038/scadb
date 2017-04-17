
package org.herry2038.scadb.db.pool

object PoolConfiguration {
  val Default = new PoolConfiguration(0, 10, 4, 10)
}

/**
 *
 * Defines specific pieces of a pool's behavior.
 *
 * @param maxObjects how many objects this pool will hold
 * @param maxIdle how long are objects going to be kept as idle (not in use by clients of the pool)
 * @param maxQueueSize when there are no more objects, the pool can queue up requests to serve later then there
 *                     are objects available, this is the maximum number of enqueued requests
 * @param validationInterval pools will use this value as the timer period to validate idle objects.
 */

case class PoolConfiguration(
                              idleObjects: Int, // 这个究竟应该不应该加上去呢 TODO
                              maxObjects: Int,
                              maxIdle: Long,
                              maxQueueSize: Int,
                              validationInterval: Long = 5000
                              )
