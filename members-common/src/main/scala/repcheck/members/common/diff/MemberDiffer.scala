package repcheck.members.common.diff

import difflicious.Differ
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
 */
object MemberDiffer {

  given Differ[MemberDO] = Differ.useEquals[MemberDO](_.toString)

}
