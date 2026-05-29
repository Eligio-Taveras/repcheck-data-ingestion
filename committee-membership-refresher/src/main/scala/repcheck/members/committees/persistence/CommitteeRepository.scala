package repcheck.members.committees.persistence

import doobie.ConnectionIO

import repcheck.members.committees.model.{CommitteeDO, CommitteeInsert}

trait CommitteeRepository {

  def upsert(committee: CommitteeInsert): ConnectionIO[CommitteeDO]

  def upsertPlaceholder(naturalKey: String, chamber: String): ConnectionIO[CommitteeDO]

  def findByCode(naturalKey: String): ConnectionIO[Option[CommitteeDO]]

  def setParent(childCode: String, parentId: Long): ConnectionIO[Unit]

  def countCurrent(): ConnectionIO[Int]

}
