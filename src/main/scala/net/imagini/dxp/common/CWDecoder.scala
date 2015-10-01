package net.imagini.dxp.common

import java.text.SimpleDateFormat

/**
 * Created by mharis on 01/10/15.
 */
class CWDecoder(date: String, mobileIdSpace: String, probabilityThreshold: Double) {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val ts = dateFormat.parse(date).getTime
  val msIdSpace = IdSpace(mobileIdSpace)
  val allSpaces = mobileIdSpace == "*"

  def decodeCWIdentifier(id: String, id_type: String): Vid = {
    try {
      id_type match {
        case "0" => Vid("a", id)
        case "1" => if (allSpaces || mobileIdSpace == "idfa") Vid("idfa", id) else null
        //case "2" => Android Advertising ID // AAID or GAID - PII cannot be used !
        //case "7" => Vid("aidsha1", id)
        //case "8" => Vid("aidmd5", id)
        case "9" => if (allSpaces || mobileIdSpace == "aaid") Vid("aaid", id) else null
        case "14" => if (allSpaces || mobileIdSpace == "waid") Vid("waid", id) else null
        case _ => null
      }
    } catch {
      case _: IllegalArgumentException => null
    }
  }

  def decodeTextLine(line: String): (Vid, (Vid, Edge)) = {
    val fields = line.split("\t")
    try {
      val p = (6.0 - fields(7).toDouble) / 5.0
      if (p >= probabilityThreshold) {
        val leftIdentifier = decodeCWIdentifier(fields(1), fields(2))
        val rightIdentifier = decodeCWIdentifier(fields(4), fields(5))
        if (leftIdentifier == null || rightIdentifier == null) {
          (null, null)
        } else {
          (leftIdentifier -> (rightIdentifier, Edge("CW", p, ts)))
        }
      } else {
        (null, null)
      }
    } catch {
      case e: IllegalArgumentException => (null, null)
    }
  }
}
