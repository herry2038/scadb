
package org.herry2038.scadb.db.util

import scala.util._
import scala.util.Success

object Version {

  private def tryParse( index : Int, pieces : Array[String] ) : Int = {

    Try {
      pieces(index).toInt
    } match {
      case Success(value) => value
      case Failure(e) => 0
    }

  }

  def apply( version : String ) : Version = {
    val pieces = version.split('.')
    new Version( tryParse(0, pieces), tryParse(1, pieces), tryParse(2, pieces) )
  }

}

case class Version( major : Int, minor : Int, maintenance : Int ) extends Ordered[Version] {
  override def compare( y: Version): Int = {

    if ( this == y ) {
      return 0
    }

    if ( this.major != y.major ) {
      return this.major.compare(y.major)
    }

    if ( this.minor != y.minor ) {
      return this.minor.compare(y.minor)
    }

    this.maintenance.compare(y.maintenance)
  }
}
