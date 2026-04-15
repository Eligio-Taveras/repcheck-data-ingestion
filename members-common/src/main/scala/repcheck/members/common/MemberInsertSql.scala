package repcheck.members.common

object MemberInsertSql {

  val value: String =
    """INSERT INTO members (
      |  natural_key, first_name, last_name, direct_order_name, inverted_order_name,
      |  honorific_name, birth_year, current_party, state, district, image_url, image_attribution,
      |  official_url, update_date
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON CONFLICT (natural_key) DO NOTHING""".stripMargin

}
