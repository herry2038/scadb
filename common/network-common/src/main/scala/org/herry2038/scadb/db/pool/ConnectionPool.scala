
package org.herry2038.scadb.db.pool

import org.herry2038.scadb.db.{QueryResult, Connection}
import org.herry2038.scadb.db.util.ExecutorServiceUtils
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent._

/**
 *
 * Pool specialized in database connections that also simplifies connection handling by
 * implementing the [[Connection]] trait and saving clients from having to implement
 * the "give back" part of pool management. This lets you do your job without having to worry
 * about managing and giving back connection objects to the pool.
 *
 * The downside of this is that you should not start transactions or any kind of long running process
 * in this object as the object will be sent back to the pool right after executing a query. If you
 * need to start transactions you will have to take an object from the pool, do it and then give it
 * back manually.
 *
 * @param factory
 * @param configuration
 */

class ConnectionPool[T <: Connection](name: String,
                      factory: ObjectFactory[T],
                      configuration: PoolConfiguration,
                      val executionContext: ExecutionContext = ExecutorServiceUtils.CachedExecutionContext
                      )
  extends SingleThreadedAsyncObjectPool[T](name, factory, configuration)
  with Connection {
  val objId = 0L
  /**
   *
   * Closes the pool, you should discard the object.
   *
   * @return
   */

  def disconnect: Future[Connection] = if ( this.isConnected ) {
    this.close.map(item => this)(executionContext)
  } else {
    Future.successful(this)
  }

  /**
   *
   * Always returns an empty map.
   *
   * @return
   */

  def connect: Future[Connection] = Future.successful(this)

  def isConnected: Boolean = !this.isClosed


  def setAutoCommit(b: Boolean) : Future[QueryResult] = future { new QueryResult(0,null) }(executionContext)

  /**
   *
   * Picks one connection and runs this query against it. The query should be stateless, it should not
   * start transactions and should not leave anything to be cleaned up in the future. The behavior of this
   * object is undefined if you start a transaction from this method.
   *
   * @param query
   * @return
   */

  def sendQuery(query: String, handler: Any = null): Future[QueryResult] =
    this.use(_.execute(query, handler)(executionContext))(executionContext)
  /**
   *
   * Picks one connection and runs this query against it. The query should be stateless, it should not
   * start transactions and should not leave anything to be cleaned up in the future. The behavior of this
   * object is undefined if you start a transaction from this method.
   *
   * @param query
   * @param values
   * @return
   */

  def sendPreparedStatement(query: String, values: Seq[Any] = List()): Future[QueryResult] =
    this.use(_.sendPreparedStatement(query, values))(executionContext)

  /**
   *
   * Picks one connection and executes an (asynchronous) function on it within a transaction block.
   * If the function completes successfully, the transaction is committed, otherwise it is aborted.
   * Either way, the connection is returned to the pool on completion.
   *
   * @param f operation to execute on a connection
   * @return result of f, conditional on transaction operations succeeding
   */

  override def inTransaction[A](f : Connection => Future[A])(implicit context : ExecutionContext = executionContext) : Future[A] =
    this.use(_.inTransaction[A](f)(context))(executionContext)


}
