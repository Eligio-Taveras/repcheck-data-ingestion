package repcheck.members.common.persistence

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.members.common.errors.InvalidBioguideId
import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.member.MemberDO

class DoobieMemberRepository extends MemberRepository {

  private val bioguideIdPattern = "^[A-Z]\\d{6}$".r

  /**
   * Explicit column list matching [[MemberDO]] constructor parameter order. Required because the `id` column was added
   * via ALTER TABLE (migration 011) and sits at the end of the physical table layout, while `MemberDO.memberId` is the
   * first field. Doobie maps columns positionally, so `SELECT *` would produce a type mismatch.
   */
  private val memberColumns: Fragment =
    fr"""id, natural_key, first_name, last_name, direct_order_name, inverted_order_name,
         honorific_name, birth_year, current_party, state, district, image_url,
         image_attribution, official_url, update_date, created_at, updated_at"""

  override def findById(memberId: Long): ConnectionIO[Option[MemberDO]] = {
    val table = Fragment.const(Tables.Members)
    (fr"SELECT" ++ memberColumns ++ fr"FROM" ++ table ++ fr"WHERE id = $memberId")
      .query[MemberDO]
      .option
  }

  override def findByBioguideId(bioguideId: String): ConnectionIO[Option[MemberDO]] = {
    val table = Fragment.const(Tables.Members)
    (fr"SELECT" ++ memberColumns ++ fr"FROM" ++ table ++ fr"WHERE natural_key = $bioguideId")
      .query[MemberDO]
      .option
  }

  override def upsert(member: MemberDO): ConnectionIO[Long] = {
    val table = Fragment.const(Tables.Members)
    sql"""INSERT INTO $table (
      natural_key, first_name, last_name, direct_order_name, inverted_order_name,
      honorific_name, birth_year, current_party, state, district,
      image_url, image_attribution, official_url, update_date
    ) VALUES (
      ${member.naturalKey}, ${member.firstName}, ${member.lastName},
      ${member.directOrderName}, ${member.invertedOrderName}, ${member.honorificName},
      ${member.birthYear}, ${member.currentParty}, ${member.state}, ${member.district},
      ${member.imageUrl}, ${member.imageAttribution}, ${member.officialUrl}, ${member.updateDate}
    )
    ON CONFLICT (natural_key) DO UPDATE SET
      first_name = EXCLUDED.first_name,
      last_name = EXCLUDED.last_name,
      direct_order_name = EXCLUDED.direct_order_name,
      inverted_order_name = EXCLUDED.inverted_order_name,
      honorific_name = EXCLUDED.honorific_name,
      birth_year = EXCLUDED.birth_year,
      current_party = EXCLUDED.current_party,
      state = EXCLUDED.state,
      district = EXCLUDED.district,
      image_url = EXCLUDED.image_url,
      image_attribution = EXCLUDED.image_attribution,
      official_url = EXCLUDED.official_url,
      update_date = EXCLUDED.update_date
    RETURNING id""".query[Long].unique
  }

  /**
   * Single-roundtrip placeholder upsert keyed on `natural_key` (the bioguide id column). The trailing `DO UPDATE SET
   * natural_key ... EXCLUDED ...` is a no-op tweak — `DO NOTHING` would skip `RETURNING id` on conflict, forcing a
   * separate SELECT. Validation runs before SQL is issued so malformed inputs raise `InvalidBioguideId` without a
   * database hit.
   */
  override def upsertPlaceholder(bioguideId: String): ConnectionIO[Long] =
    if (bioguideIdPattern.matches(bioguideId)) {
      val table = Fragment.const(Tables.Members)
      sql"""INSERT INTO $table (natural_key) VALUES ($bioguideId)
            ON CONFLICT (natural_key) DO UPDATE SET natural_key = EXCLUDED.natural_key
            RETURNING id""".query[Long].unique
    } else {
      doobie.free.connection.raiseError[Long](InvalidBioguideId(bioguideId))
    }

  override def findPlaceholders(): ConnectionIO[List[MemberDO]] = {
    val table = Fragment.const(Tables.Members)
    (fr"SELECT" ++ memberColumns ++ fr"FROM" ++ table ++ fr"WHERE update_date IS NULL")
      .query[MemberDO]
      .to[List]
  }

  override def existsWithLisMapping(memberId: Long): ConnectionIO[Boolean] = {
    val mappingTable = Fragment.const(Tables.MemberLisMapping)
    sql"SELECT EXISTS(SELECT 1 FROM $mappingTable WHERE member_id = $memberId)"
      .query[Boolean]
      .unique
  }

}
