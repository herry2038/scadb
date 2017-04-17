
package org.herry2038.scadb.db.exceptions

/**
 *
 * Raised when the user gives more or less parameters than the query takes. Each parameter is a ?
 * (question mark) in the query string. The count of ? should be the same as the count of items in the provided
 * sequence of parameters.
 *
 * @param expected the expected count of parameters
 * @param given the collection given
 */
class InsufficientParametersException( expected : Int, given : Seq[Any] )
  extends DatabaseException(
    "The query contains %s parameters but you gave it %s (%s)".format(expected, given.length, given.mkString(",")
    )
  )
