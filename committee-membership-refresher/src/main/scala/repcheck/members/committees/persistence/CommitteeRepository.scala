package repcheck.members.committees.persistence

import doobie.ConnectionIO

import repcheck.members.committees.model.{CommitteeDO, CommitteeInsert}

trait CommitteeRepository {

  def upsert(committee: CommitteeInsert): ConnectionIO[CommitteeDO]

  def upsertPlaceholder(naturalKey: String, chamber: String): ConnectionIO[CommitteeDO]

  def findByCode(naturalKey: String): ConnectionIO[Option[CommitteeDO]]

  /** All committees (used by the historical loader to drive resolution off the DB's committee universe). */
  def listAll(): ConnectionIO[List[CommitteeDO]]

  def setParent(childCode: String, parentId: Long): ConnectionIO[Unit]

  def countCurrent(): ConnectionIO[Int]

}
