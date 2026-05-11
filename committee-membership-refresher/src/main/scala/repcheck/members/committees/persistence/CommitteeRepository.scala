package repcheck.members.committees.persistence

import doobie.ConnectionIO

import repcheck.members.committees.model.CommitteeDO

trait CommitteeRepository {

  def upsert(committee: CommitteeDO): ConnectionIO[Unit]

  def upsertPlaceholder(committeeCode: String, chamber: String): ConnectionIO[Unit]

  def findByCode(committeeCode: String): ConnectionIO[Option[CommitteeDO]]

  def findAllSenateParentCodes(): ConnectionIO[List[String]]

  def setParent(childCode: String, parentCode: String): ConnectionIO[Unit]

}
