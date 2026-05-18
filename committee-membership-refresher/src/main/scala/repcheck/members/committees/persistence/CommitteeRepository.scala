package repcheck.members.committees.persistence

import doobie.ConnectionIO

import repcheck.members.committees.model.CommitteeDO

trait CommitteeRepository {

  def upsert(committee: CommitteeDO): ConnectionIO[CommitteeDO]

  def upsertPlaceholder(naturalKey: String, chamber: String): ConnectionIO[CommitteeDO]

  def findByCode(naturalKey: String): ConnectionIO[Option[CommitteeDO]]

  def findAllSenateParentCodes(): ConnectionIO[List[String]]

  def setParent(childCode: String, parentId: Long): ConnectionIO[Unit]

  def countCurrent(): ConnectionIO[Int]

}
