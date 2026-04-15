package repcheck.members.common.persistence

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.member.MemberDO

class DoobieMemberRepository extends MemberRepository {

  override def findById(memberId: Long): ConnectionIO[Option[MemberDO]] = {
    val table = Fragment.const(Tables.Members)
    sql"SELECT * FROM $table WHERE id = $memberId"
      .query[MemberDO]
      .option
  }

  override def findByBioguideId(bioguideId: String): ConnectionIO[Option[MemberDO]] = {
    val table = Fragment.const(Tables.Members)
    sql"SELECT * FROM $table WHERE natural_key = $bioguideId"
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

  override def findPlaceholders(): ConnectionIO[List[MemberDO]] = {
    val table = Fragment.const(Tables.Members)
    sql"SELECT * FROM $table WHERE update_date IS NULL"
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
