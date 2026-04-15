package repcheck.members.common

/**
 * INSERT statement for placeholder member creation via
 * [[repcheck.ingestion.common.placeholders.DoobieEntityRepository]]. Uses ON CONFLICT DO NOTHING because placeholders
 * must not overwrite existing members — whether they are fully populated profiles or earlier placeholders. The owning
 * pipeline (member-profile-pipeline) fills in the real data later via
 * [[repcheck.members.common.persistence.MemberRepository.upsert]].
 */
object MemberInsertSql {

  val value: String =
    """INSERT INTO members (
      |  natural_key, first_name, last_name, direct_order_name, inverted_order_name,
      |  honorific_name, birth_year, current_party, state, district, image_url, image_attribution,
      |  official_url, update_date
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON CONFLICT (natural_key) DO NOTHING""".stripMargin

}
