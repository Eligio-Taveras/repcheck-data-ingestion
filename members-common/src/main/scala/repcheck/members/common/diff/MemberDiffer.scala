package repcheck.members.common.diff

import difflicious.Differ
import repcheck.shared.models.congress.common.{Party, UsState}
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
 * Custom `Differ` instances for `Party` and `UsState` enums compare by value equality, since difflicious does not
 * auto-derive for enums without an explicit instance.
 */
object MemberDiffer {

  given Differ[Party]   = Differ.useEquals[Party](_.toString)
  given Differ[UsState] = Differ.useEquals[UsState](_.toString)

  given Differ[MemberDO] = Differ.useEquals[MemberDO](_.toString)

}
