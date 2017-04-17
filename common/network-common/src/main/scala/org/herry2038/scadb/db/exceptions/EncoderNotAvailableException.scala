
package org.herry2038.scadb.db.exceptions

import org.herry2038.scadb.db.KindedMessage

class EncoderNotAvailableException(message: KindedMessage)
  extends DatabaseException("Encoder not available for name %s".format(message.kind))
