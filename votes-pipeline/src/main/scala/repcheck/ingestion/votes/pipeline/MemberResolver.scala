package repcheck.ingestion.votes.pipeline

import cats.effect.Async
import cats.syntax.all._

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.ingestion.votes.errors.MemberResolutionFailed
import repcheck.shared.models.congress.dos.member.MemberDO

/**
 * Resolves House `bioguideID` strings to internal `members.id` Long PKs for vote-position writes.
 *
 * ==Flow==
 *   1. `PlaceholderCreator.ensureExists[MemberDO]` performs an idempotent insert-if-not-exists on `members` keyed by
 *      the bioguide natural key. No-op when the row already exists; otherwise writes a stub `MemberDO` whose
 *      placeholder fields are overwritten by the next members-pipeline run. 2. `findMemberIdByBioguide(bioguide)` reads
 *      back the surrogate `members.id`. Supplied as a callback so the resolver stays decoupled from the concrete Doobie
 *      repository + transactor at compile time; in production it is wired as `bid =>
 *      memberRepo.findByBioguideId(bid).map(_.map(_.memberId)).transact(xa)`, in tests it is a plain closure returning
 *      canned responses.
 *
 * If `findMemberIdByBioguide` returns `None` after `ensureExists` succeeds, the resolver raises
 * [[MemberResolutionFailed]] rather than silently producing a missing identity. That scenario is pathological (would
 * require another actor to delete the row between the two calls) but we surface it as a per-vote failure so the stream
 * keeps processing other votes.
 *
 * Senate positions do NOT go through this resolver — they carry `lis_member_id` and bypass the `members` table per the
 * migration 023 dual-identity design. Only House positions are translated here.
 */
private[pipeline] class MemberResolver[F[_]: Async](
  findMemberIdByBioguide: String => F[Option[Long]],
  placeholderCreator: PlaceholderCreator[F],
  memberEntityRepo: EntityRepository[F, MemberDO],
  logger: PipelineLogger[F],
) {

  /**
   * Resolve a single bioguide id to an internal `members.id`. Creates a placeholder first, then looks up. Raises
   * [[MemberResolutionFailed]] if the lookup returns `None` after the placeholder write succeeded.
   */
  def resolveBioguide(bioguideId: String, logCtx: LogContext): F[Long] =
    for {
      _       <- placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)
      maybeId <- findMemberIdByBioguide(bioguideId)
      resolvedId <- maybeId match {
        case Some(id) => Async[F].pure(id)
        case None =>
          val err = MemberResolutionFailed(
            bioguideId = bioguideId,
            detail = "findMemberIdByBioguide returned None after ensureExists — placeholder row disappeared",
          )
          logger.error(logCtx, err.getMessage, Some(err)) *> Async[F].raiseError[Long](err)
      }
    } yield resolvedId

  /**
   * Batch version. Resolves each distinct bioguide at most once and returns a `Map[bioguide, memberId]` for the caller
   * to thread into position DOs. Deduplication is on the caller side's responsibility (the processor dedupes via the
   * DTO-level position list), but the resolver also defensively distincts the input to avoid redundant DB round-trips
   * on a duplicated feed.
   */
  def resolveBatch(bioguideIds: List[String], logCtx: LogContext): F[Map[String, Long]] =
    bioguideIds.distinct
      .traverse(bid => resolveBioguide(bid, logCtx).map(id => bid -> id))
      .map(_.toMap)

}
