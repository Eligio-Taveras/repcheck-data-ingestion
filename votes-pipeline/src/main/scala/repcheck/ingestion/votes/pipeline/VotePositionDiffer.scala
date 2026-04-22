package repcheck.ingestion.votes.pipeline

import difflicious.Differ
import difflicious.implicits._
import repcheck.shared.models.congress.dos.vote.VotePositionDO

/**
 * Provides a `Differ[List[VotePositionDO]]` instance that treats the position list as a **set keyed by the dual
 * identity `(memberId, lisMemberId)`**. Called by [[VoteChangeDetector]] to decide whether an incoming vote's positions
 * differ from the stored set.
 *
 * ==Dual identity key==
 *
 * Per migration 023 / shared-models 0.1.29, a row populates EITHER `memberId` (House, bioguide-resolved to
 * `members.id`) OR `lisMemberId` (Senate, `lis_members.id`). The detector pairs incoming vs. stored positions by the
 * composite `(memberId, lisMemberId)` tuple — which is unique per row by the DB's `chk_vp_xor_identity` CHECK — so the
 * same member showing up with a different `position` renders as a `Both` value-diff rather than an added/removed pair.
 *
 * ==Why `useEquals` + `.pairBy`==
 *
 * Uses `Differ.useEquals` rather than `Differ.derived` for the element-level `Differ[VotePositionDO]`. Per
 * `MemberDiffer`, value-equality is sufficient for our needs and avoids the hundreds of macro-expanded field branches
 * `Differ.derived` emits, which can never be exercised at runtime and drag coverage below the 95% gate.
 *
 * ==Produced `DiffResult` shape==
 *
 * Calling `summon[Differ[List[VotePositionDO]]].diff(incoming, stored)` returns a `DiffResult.ListResult` whose `items:
 * Vector[DiffResult.ValueResult]` encodes the per-member verdicts:
 *
 *   - `ValueResult.ObtainedOnly(...)` — member present in incoming but not stored → "Added"
 *   - `ValueResult.ExpectedOnly(...)` — member present in stored but not incoming → "Removed"
 *   - `ValueResult.Both(_, _, isSame = true, _)` — same member, identical `VotePositionDO.toString` → unchanged
 *   - `ValueResult.Both(_, _, isSame = false, _)` — same member, different cast → "Changed"
 *
 * `listResult.isOk` is `true` iff every item is `isOk` (either identical `Both` or ignored). The detector uses that to
 * derive `positionsChanged = !diffResult.isOk`.
 */
object VotePositionDiffer {

  /**
   * Element-level differ built on value equality. `VotePositionDO` already has a full case-class `equals` (data class)
   * so this compares every field — including `position`, `partyAtVote`, `stateAtVote`, and both identity columns. The
   * `.toString` is used only as the printable rendering inside `ValueResult`.
   */
  given votePositionDiffer: Differ[VotePositionDO] =
    Differ.useEquals[VotePositionDO](_.toString)

  /**
   * List differ keyed by the dual identity tuple `(memberId, lisMemberId)` — the canonical identity for a position
   * within a single vote. `Differ.seqDiffer` is invoked directly rather than via `summon` to avoid a self-referential
   * resolution against this very `given`.
   */
  given votePositionsDiffer: Differ[List[VotePositionDO]] =
    Differ.seqDiffer[List, VotePositionDO].pairBy(p => (p.memberId, p.lisMemberId))

}
