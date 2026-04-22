package repcheck.ingestion.votes.pipeline

import difflicious.Differ
import difflicious.implicits._
import repcheck.shared.models.congress.dos.vote.VotePositionDO

/**
 * Provides a `Differ[List[VotePositionDO]]` instance that treats the position list as a **set keyed by the populated
 * identity** (`memberId` XOR `lisMemberId`). Called by [[VoteChangeDetector]] to decide whether an incoming vote's
 * positions differ from the stored set.
 *
 * ==Why `useEquals` + `.pairBy`==
 *
 * Uses `Differ.useEquals` rather than `Differ.derived` for the element-level `Differ[VotePositionDO]`. Per
 * `MemberDiffer`, value-equality is sufficient for our needs and avoids the hundreds of macro-expanded field branches
 * `Differ.derived` emits, which can never be exercised at runtime and drag coverage below the 95% gate. The
 * `.pairBy(identityKey)` lifts the list differ from the default index-based comparison to identity-keyed matching — so
 * order is irrelevant and the same member with a different `position` renders as a `Both` value-diff rather than a
 * spurious added/removed pair.
 *
 * ==Identity key post-migration 023==
 *
 * Each `VotePositionDO` carries either a House-arm `memberId: Option[Long]` or a Senate-arm `lisMemberId: Option[Long]`
 * (per the XOR invariant). To build a single comparison key per row, we project to `("M", memberId)` for House and
 * `("L", lisMemberId)` for Senate — the tag prevents a House row with `memberId = 5L` from colliding with a Senate row
 * whose `lisMemberId = 5L`. Rows that somehow violate the XOR (both `None` or both `Some`) degenerate to `("?", 0L)`
 * and are still comparable via the element-level `useEquals`.
 *
 * ==Produced `DiffResult` shape==
 *
 * Calling `summon[Differ[List[VotePositionDO]]].diff(incoming, stored)` returns a `DiffResult.ListResult` whose `items:
 * Vector[DiffResult.ValueResult]` encodes the per-identity verdicts:
 *
 *   - `ValueResult.ObtainedOnly(...)` — identity present in incoming but not stored → "Added"
 *   - `ValueResult.ExpectedOnly(...)` — identity present in stored but not incoming → "Removed"
 *   - `ValueResult.Both(_, _, isSame = true, _)` — same identity, identical `VotePositionDO.toString` → unchanged
 *   - `ValueResult.Both(_, _, isSame = false, _)` — same identity, different cast → "Changed"
 *
 * `listResult.isOk` is `true` iff every item is `isOk` (either identical `Both` or ignored). The detector uses that to
 * derive `positionsChanged = !diffResult.isOk`.
 */
object VotePositionDiffer {

  /**
   * Element-level differ built on value equality. `VotePositionDO` already has a full case-class `equals` (data class)
   * so this compares every field — including `position`, `partyAtVote`, `stateAtVote`, and both identity columns.
   * `.toString` is used only as the printable rendering inside `ValueResult`.
   */
  given votePositionDiffer: Differ[VotePositionDO] =
    Differ.useEquals[VotePositionDO](_.toString)

  /**
   * Builds the identity pair-key for a single position row. House rows are tagged `"M"`, Senate rows `"L"`; the tag
   * prevents identity collisions between a member and an LIS senator that happen to share a numeric id.
   */
  private[pipeline] def identityKey(p: VotePositionDO): (String, Long) =
    (p.memberId, p.lisMemberId) match {
      case (Some(id), None) => ("M", id)
      case (None, Some(id)) => ("L", id)
      case _                => ("?", 0L)
    }

  /**
   * List differ keyed by the chamber-tagged identity pair. Built from [[votePositionDiffer]] via difflicious's
   * `seqDiffer` implicit, then reconfigured with `.pairBy(identityKey)` to get set-style matching. `Differ.seqDiffer`
   * is invoked directly rather than via `summon` to avoid a self-referential resolution against this very `given`.
   */
  given votePositionsDiffer: Differ[List[VotePositionDO]] =
    Differ.seqDiffer[List, VotePositionDO].pairBy(identityKey)

}
