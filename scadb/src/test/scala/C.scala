import org.herry2038.scadb.util.Logging

/**
 * Created by Administrator on 2017/6/1.
 */
class C extends Logging {
  println(loggerName)
}

object C {
  def main(args: Array[String]) {
    val b = new C
    b.info("abc")
  }
}
