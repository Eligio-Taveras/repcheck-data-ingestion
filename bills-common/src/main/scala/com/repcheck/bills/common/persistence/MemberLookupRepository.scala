package com.repcheck.bills.common.persistence

trait MemberLookupRepository[F[_]] {
  def findIdByNaturalKey(naturalKey: String): F[Option[Long]]
}
