package com.repcheck.bills.common.persistence

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables

class DoobieMemberLookupRepository extends MemberLookupRepository[ConnectionIO] {

  override def findIdByNaturalKey(naturalKey: String): ConnectionIO[Option[Long]] = {
    val table = Fragment.const(Tables.Members)
    sql"SELECT id FROM $table WHERE natural_key = $naturalKey"
      .query[Long]
      .option
  }

}
