import org.apache.log4j.Logger

/**
 * Created by Administrator on 2017/6/1.
 */
class A {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
}
