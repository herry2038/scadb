
package org.herry2038.scadb.db.pool

import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicLong
import java.util.{TimerTask, Timer}
import org.herry2038.scadb.db.Connection
import org.herry2038.scadb.db.pool.AsyncObjectPool
import org.herry2038.scadb.db.util.Log

import scala.StringBuilder
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.util.{Failure, Success}
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.control.Breaks._

object MultiThreadedAsyncObjectPool {
  val Counter = new AtomicLong()
  val log = Log.get[MultiThreadedAsyncObjectPool[Nothing]]
}

/**
 *
 * Implements an [[AsyncObjectPool]] using a single thread from a
 * fixed executor service as an event loop to cause all calls to be sequential.
 *
 * Once you are done with this object remember to call it's close method to clean up the thread pool and
 * it's objects as this might prevent your application from ending.
 *
 * @param factory
 * @param configuration
 * @tparam T type of the object this pool holds
 */

class MultiThreadedAsyncObjectPool[T <: Connection](val name: String,
                                        factory: ObjectFactory[T],
                                        configuration: PoolConfiguration,
                                        executionContext : ExecutionContext
                                        ) extends AsyncObjectPool[T] {
  import MultiThreadedAsyncObjectPool.log
  private val poolables = new ConcurrentLinkedQueue[PoolableHolder[T]]()
  private val checkouts = new ConcurrentHashMap[Long,T]()
  private val waitQueue = new mutable.ArrayBuffer[Promise[T]](configuration.maxQueueSize)

  private val Counter = new AtomicLong()

  implicit val executionContextService = executionContext


  private val timer = new Timer("async-object-pool-timer-" + Counter.incrementAndGet(), true)
  timer.schedule(new TimerTask {
    def run() {
      action {
        testObjects
      }
    }
  }, configuration.validationInterval, configuration.validationInterval)


  def status = {
    val builder = new StringBuilder().append(configuration.maxObjects).append('-').append(configuration.idleObjects)
      .append('-').append(configuration.maxQueueSize).append('-')
      .append(this.poolables.size).append('-')
      .append(this.checkouts.size).append('-')
    this.synchronized {
      builder.append(this.waitQueue.size)
    }
    builder.toString
  }


  private var closed = false


  def action(f: => Unit) {
    this.executionContext.execute(new Runnable {
      def run() {
        try {
          f
        } catch {
          case e : Exception => {
            log.error("Failed to execute task %s".format(f), e)
          }
        }
      }
    })
  }

  /**
   *
   * Asks for an object from the pool, this object should be returned to the pool when not in use anymore.
   *
   * @return
   */

  def take: Future[T] = {

    if (this.closed) {
      return Promise.failed(new PoolAlreadyTerminatedException).future
    }

    val promise = Promise[T]()
    this.checkout(promise)
    promise.future
  }

  /**
   *
   * Returns an object to the pool. The object is validated before being added to the collection
   * of available objects to make sure we have a usable object. If the object isn't valid it's discarded.
   *
   * @param item
   * @return
   */

  def giveBack(item: T): Future[AsyncObjectPool[T]] = {
    val promise = Promise[AsyncObjectPool[T]]()

    this.checkouts.remove(item.objId)
    this.factory.validate(item) match {
      case Success(item) => {
        this.addBack(item, promise)
      }
      case Failure(e) => {
        this.factory.destroy(item)
        this.addBack(null.asInstanceOf[T], promise)
        promise.failure(e)
      }
    }

    promise.future
  }

  def isFull: Boolean = // this.poolables.isEmpty && this.checkouts.size >= configuration.maxObjects
    this.checkouts.size >= configuration.maxObjects

  def  close: Future[AsyncObjectPool[T]] = {
    val promise = Promise[AsyncObjectPool[T]]()


    if (!this.closed) {
      try {
        this.timer.cancel()
        this.closed = true
        while ( !this.poolables.isEmpty ) {
          for ( holder <- poolables ) {
            factory.destroy(holder.item)
          }
        }

        while ( ! this.checkouts.isEmpty ) {
          for ( key <- this.checkouts.keys() ) {
            Option(this.checkouts.remove(key)).map(item => factory.destroy(item))
          }
        }

        while ( !this.poolables.isEmpty ) {
          for ( holder <- poolables ) {
            factory.destroy(holder.item)
          }
        }

        while ( ! this.checkouts.isEmpty ) {
          for ( key <- this.checkouts.keys() ) {
            Option(this.checkouts.remove(key)).map(item => factory.destroy(item))
          }
        }

        promise.success(this)
      } catch {
        case e: Exception => promise.failure(e)
      }
    } else {
      promise.success(this)
    }


    promise.future
  }

  def availables: Traversable[T] = this.poolables.map(item => item.item)

  def inUse: Traversable[T] = this.checkouts.values

  def queued: Traversable[Promise[T]] = this.waitQueue

  def isClosed: Boolean = this.closed

  /**
   *
   * Adds back an object that was in use to the list of poolable objects.
   *
   * @param item
   * @param promise
   */

  private def addBack(item: T, promise: Promise[AsyncObjectPool[T]]) {
    if ( item != null )
      this.poolables.add(new PoolableHolder[T](item))

    this.synchronized {
      if (!this.waitQueue.isEmpty) {
        this.checkout(this.waitQueue.remove(0))
      }
    }

    if ( item != null )
      promise.success(this)
  }

  /**
   *
   * Enqueues a promise to be fulfilled in the future when objects are sent back to the pool. If
   * we have already reached the limit of enqueued objects, fail the promise.
   *
   * @param promise
   */

  private def enqueuePromise(promise: Promise[T]) {
    this.synchronized {
      if (this.waitQueue.size >= configuration.maxQueueSize) {
        val exception = new PoolExhaustedException("There are no objects available and the waitQueue is full")
        exception.fillInStackTrace()
        promise.failure(exception)
      } else {
        this.waitQueue += promise
      }
    }
  }

  private def checkout(promise: Promise[T]) {
    if (this.isFull) {
      this.enqueuePromise(promise)
    } else {
      this.createOrReturnItem(promise)
    }
  }

  /**
   *
   * Checks if there is a poolable object available and returns it to the promise.
   * If there are no objects available, create a new one using the factory and return it.
   *
   * @param promise
   */

  private def createOrReturnItem(promise: Promise[T]) {
    var holder = this.poolables.poll()
    while (holder != null ) {
      val item = holder.item
      if (item.isConnected) {
        this.checkouts += (item.objId -> item)
        promise.success(item)
        return // 找到合适的连接了，退出该方法
      } else {
        item.disconnect // 这个似乎不需要
      }
      holder = this.poolables.poll()
    }

    // 池子里面找不到了
    try {
      val itemFuture = this.factory.create(Counter.incrementAndGet())
      itemFuture map { item =>
        this.checkouts += (item.objId -> item)
        promise.success(item)
      } recover {
        // 添加日期: 20160302 原因，有可能连接失败，这时候需要能够捕获异常。
        case e: Exception => promise.failure(e)
      }
    } catch {
      case e: Exception => promise.failure(e)
    }
  }

  override def finalize() {
    this.close
  }

  /**
   *
   * Validates pooled objects not in use to make sure they are all usable, great if
   * you're holding onto network connections since you can "ping" the destination
   * to keep the connection alive.
   *
   */

  override def testObjects {
    val removals = new ArrayBuffer[Connection]()
    val size = this.poolables.size()

    var toRemove = if ( size - configuration.idleObjects > 0 ) (size - configuration.idleObjects)/10 else 0

    log.info(s"pool [${name}] - [CONF:${configuration.maxObjects}-${configuration.idleObjects}] validating objects, current idle size:{}, checkout:${checkouts.size}, will remove:{}!!!", size, toRemove)
    breakable {
      for ( i <- 0 until size) {
        val poolable = poolables.poll()
        if (poolable != null) {
          this.factory.test(poolable.item) match {
            case Success(item) => {
              if (poolable.timeElapsed > configuration.maxIdle || toRemove > 0) {
                log.info(s"pool [${name}]Connection was idle for {}, maxIdle is {}, removing it", poolable.timeElapsed, configuration.maxIdle)
                factory.destroy(poolable.item)
                if ( toRemove > 0 ) toRemove -= 1
              } else {
                log.debug(s"pool [${name}]Connection test success maxIdle:{}, elapsed:{}!", configuration.maxIdle, poolable.timeElapsed)
                this.poolables.add(poolable)
              }
            }
            case Failure(e) => {
              log.info(s"pool [${name}]Connection test failed!", e)
              log.error("Failed to validate object", e)
              factory.destroy(poolable.item)
            }
          }
        } else {
          break
        }
      }
    }
  }

  private class PoolableHolder[T](val item: T) {
    val time = System.currentTimeMillis()

    def timeElapsed = System.currentTimeMillis() - time
  }

}
