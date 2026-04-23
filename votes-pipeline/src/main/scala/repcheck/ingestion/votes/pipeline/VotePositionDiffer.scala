package repcheck.ingestion.votes.pipeline

import difflicious.Differ
import difflicious.implicits._
import repcheck.shared.models.congress.common.Chamber
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
 * (per the XOR invariant enforced by the DB `chk_vp_xor_identity` CHECK). To build a single comparison key per row, we
 * project to `(Chamber.House, memberId)` for House and `(Chamber.Senate, lisMemberId)` for Senate — the tag prevents a
 * House row with `memberId = 5L` from colliding with a Senate row whose `lisMemberId = 5L`. Rows that violate the XOR
 * (both `None` or both `Some`) are a contract violation — migration 023 keeps them out of the database, and the
 * repository layer validates inbound DOs before persisting. If one still reaches the differ, [[identityKey]] raises
 * [[VotePositionIdentityInvalid]] so the diagnostic surfaces immediately rather than degenerating into a silent
 * sentinel tuple.
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
   * Element-level differ built on value equality. `VotePositionDO` has a case-class `.equals`, so this compares every
   * field. Callers are expected to [[normalizeForComparison]] both sides before invoking the outer
   * `Differ[List[VotePositionDO]]` — see below for why.
   *
   * ==Why normalisation is required==
   *
   * Two `VotePositionDO` fields are populated from the database rather than from the DTO/builder pipeline: `id`
   * (BIGSERIAL, 0 on freshly-built positions vs the real row id on stored positions) and `createdAt` (None on
   * freshly-built vs `Some(DB NOW())` on stored). These will ALWAYS differ between the incoming and stored sides of a
   * detect() call, so an unadjusted `.equals` reports every `Updated` branch as `positionsChanged = true` — even when
   * the actual voteCast / party / state are byte-identical. Downstream consequence: every metadata-only update
   * spuriously emits a `VoteRecordedEvent` with `isUpdate = true`, violating §6.5 AC "Updated vote metadata-only →
   * archived + upserted, no event" and forcing the scoring engine to re-process unchanged votes.
   *
   * The detector (`VoteChangeDetector.diffPositions`) normalises both sides via [[normalizeForComparison]] before
   * handing them to the list Differ. The `.equals` then compares only the vote-content fields (`position`,
   * `partyAtVote`, `stateAtVote`, plus the identity columns that the outer `.pairBy(identityKey)` already partitions
   * on). `.toString` is used only for the printable rendering inside `ValueResult`.
   */
  given votePositionDiffer: Differ[VotePositionDO] =
    Differ.useEquals[VotePositionDO](_.toString)

  /**
   * Zero out DB-generated fields (`id` and `createdAt`) so equality comparison reflects vote-content changes only.
   * Applied to both the incoming and the stored position lists inside [[VoteChangeDetector.diffPositions]] before the
   * outer list Differ is invoked.
   */
  def normalizeForComparison(p: VotePositionDO): VotePositionDO =
    p.copy(id = 0L, createdAt = None)

  /**
   * Builds the identity pair-key for a single position row. House rows project to `(Chamber.House, memberId)`, Senate
   * rows to `(Chamber.Senate, lisMemberId)`; the chamber tag prevents identity collisions between a member and an LIS
   * senator that happen to share a numeric id.
   *
   * ==Upstream contract==
   *
   * The XOR invariant (exactly one of `memberId` / `lisMemberId` populated) is guaranteed by every code path that
   * reaches this function: migration 023's `chk_vp_xor_identity` CHECK prevents malformed rows at the database
   * boundary; [[DoobieVotePositionRepository.validatePositions]] raises
   * [[repcheck.ingestion.votes.errors.VotePositionIdentityInvalid]] before inserting; and [[VoteChangeDetector]]
   * validates both sides of an in-memory position list before invoking the differ. The `sys.error` branch below is
   * therefore unreachable under the repository's public contract — it exists only as a loud fail-fast should a future
   * regression break the invariant. A pure [[Differ.pairBy]] cannot carry an `F[_]` effect, so raising a project
   * exception here would require suppressing WartRemover's `Wart.Throw`; callers that need structured failure handling
   * go through the detector's upstream validation, which raises the project exception in `F[_]` context.
   */
  private[pipeline] def identityKey(p: VotePositionDO): (Chamber, Long) =
    (p.memberId, p.lisMemberId) match {
      case (Some(id), None) => (Chamber.House, id)
      case (None, Some(id)) => (Chamber.Senate, id)
      case _ =>
        sys.error(
          s"VotePositionDO violates XOR identity invariant: voteId=${p.voteId.toString} " +
            s"memberId=${p.memberId.toString} lisMemberId=${p.lisMemberId.toString}"
        )
    }

  /**
   * List differ keyed by the chamber-tagged identity pair. Built from [[votePositionDiffer]] via difflicious's
   * `seqDiffer` implicit, then reconfigured with `.pairBy(identityKey)` to get set-style matching. `Differ.seqDiffer`
   * is invoked directly rather than via `summon` to avoid a self-referential resolution against this very `given`.
   */
  given votePositionsDiffer: Differ[List[VotePositionDO]] =
    Differ.seqDiffer[List, VotePositionDO].pairBy(identityKey)

}
