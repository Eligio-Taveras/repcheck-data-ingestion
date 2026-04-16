package repcheck.members.common.persistence

/**
 * Archive-before-overwrite pattern for members: snapshots the current member state (plus its term rows) into history
 * tables BEFORE the upsert writes new data. The live `members` table always holds the latest version; `member_history`
 * preserves each prior version. The caller is responsible for calling `archiveMember` before the upsert — the archiver
 * does not modify the live member row.
 */
trait MemberHistoryArchiver[F[_]] {
  def archiveMember(bioguideId: String): F[Unit]
}
