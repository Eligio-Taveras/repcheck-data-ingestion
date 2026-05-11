package repcheck.members.committees.model

/**
 * Normalizes committee codes to clerk format (uppercase).
 *
 * Congress.gov `systemCode` uses lowercase prefixed codes:
 *   - House: `hsru00` -> strip `hs`, uppercase -> `RU00`
 *   - Senate: `ssra00` -> uppercase -> `SSRA00`
 *   - Joint: `jjec00` -> uppercase -> `JJEC00`
 *
 * House clerk XML already uses clerk format (`RU00`). Senate XML already uses uppercase with prefix (`SSRA00`).
 */
object CommitteeCodeNormalizer {

  def fromCongressGov(systemCode: String, chamber: String): String = {
    val upper = systemCode.toUpperCase
    if (chamber.equalsIgnoreCase("House") && upper.startsWith("HS")) {
      upper.drop(2)
    } else {
      upper
    }
  }

  def fromHouseClerk(code: String): String =
    code.toUpperCase

  def fromSenateXml(code: String): String =
    code.toUpperCase

}
