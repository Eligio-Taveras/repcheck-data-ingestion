package repcheck.members.common.diff

import difflicious.{DiffResult, Differ}
import repcheck.shared.models.congress.dos.member.MemberDO

/**
 * Provides a `Differ[MemberDO]` instance for use with `ChangeDetector` in the member profile processor to detect
 * changes between the incoming API response and the stored member record.
 *
 * Uses `Differ.useEquals` rather than `Differ.derived` because the downstream `ChangeDetector` (see ingestion-common
 * §3.3) iterates fields via `Product` directly — it doesn't need difflicious's field-level diff tree. A value-equality
 * instance is sufficient and avoids the hundreds of macro-expanded branches that `Differ.derived` emits, which can
 * never be exercised at runtime and drag coverage below the global threshold.
 *
 * Because `useEquals` compares via `equals` without decomposing the case class into child fields, no per-field `Differ`
 * instances (for `Party`, `UsState`, etc.) are required.
 *
 * Identity-column handling: callers that want to compare members ignoring DB-managed fields (`memberId`, `createdAt`,
 * `updatedAt`) should use [[diffIgnoringIdentity]] rather than the raw `given Differ[MemberDO]`. This is the equivalent
 * of difflicious's `.ignoreAt`, which is only available on `Differ.derived` instances — and we deliberately avoid
 * `derived` here for the coverage reason above.
 */
object MemberDiffer {

  given Differ[MemberDO] = Differ.useEquals[MemberDO](_.toString)

  /**
   * Compares two `MemberDO` values while ignoring identity columns (`memberId`, `createdAt`, `updatedAt`). These are
   * DB-managed (BIGSERIAL PK and timestamp triggers) and would otherwise produce false-positive change detections
   * whenever an incoming API DTO is compared against a stored row. Equivalent to the difflicious
   * `Differ.derived[MemberDO].ignoreAt(_.memberId).ignoreAt(_.createdAt).ignoreAt(_.updatedAt)` pattern, but built on
   * `useEquals` so we keep coverage out of the macro-derived branch explosion.
   */
  def diffIgnoringIdentity(a: MemberDO, b: MemberDO): DiffResult =
    summon[Differ[MemberDO]].diff(normalize(a), normalize(b))

  private def normalize(m: MemberDO): MemberDO =
    m.copy(memberId = 0L, createdAt = None, updatedAt = None)

}
