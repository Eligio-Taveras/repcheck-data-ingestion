package repcheck.members.common.diff

import difflicious.Differ
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.MemberDO

/**
 * Provides a `Differ[MemberDO]` instance using difflicious auto-derivation. Used by `ChangeDetector` in the member
 * profile processor to detect field-level changes between the incoming API response and the stored member record.
 *
 * Custom `Differ` instances for `Party` and `UsState` enums compare by value equality, since difflicious does not
 * auto-derive for enums without an explicit instance.
 */
object MemberDiffer {

  given Differ[Party]   = Differ.useEquals[Party](_.toString)
  given Differ[UsState] = Differ.useEquals[UsState](_.toString)

  given Differ[MemberDO] = Differ.derived[MemberDO]

}
