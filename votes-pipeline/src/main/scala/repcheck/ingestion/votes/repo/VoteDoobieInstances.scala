package repcheck.ingestion.votes.repo

import java.time.{LocalDate, ZoneOffset}

import doobie.postgres.implicits.JavaTimeInstantMeta
import doobie.{Get, Meta, Put}

/**
 * Bridges the schema-vs-DO type mismatch on `votes.vote_date` (and its history table mirrors). The column is
 * `TIMESTAMPTZ` per migration 001 but [[repcheck.shared.models.congress.dos.vote.VoteDO]] models `voteDate` as
 * `Option[LocalDate]` — the domain cares only about the date, not the wall-clock time.
 *
 * We therefore rebuild `Meta[LocalDate]` over the PostgreSQL `timestamptz` shape (which surfaces through JDBC as
 * `Instant`): on READ the instant is truncated to `ZoneOffset.UTC.atDate` → `LocalDate`, and on WRITE a `LocalDate` is
 * expanded to `Instant.atStartOfDay(UTC)`. This preserves identity across `insert / read` round-trips as long as all
 * writes flow through Doobie (any externally-written row that stamps a non-midnight time will lose its time component
 * on read, which is the desired domain semantics).
 *
 * This instance shadows the default `Meta[LocalDate]` from `doobie.postgres.implicits._` which targets the `DATE`
 * column type. `DoobieVoteRepository` and `DoobieVotePositionRepository` import it explicitly; callers that need the
 * `DATE`-backed behavior must not import this file.
 */
object VoteDoobieInstances {

  private val utc: ZoneOffset = ZoneOffset.UTC

  /**
   * Maps the PostgreSQL `timestamptz` column to `LocalDate` via the UTC calendar. The intermediate representation is
   * `java.time.OffsetDateTime` — JDBC's preferred Java type for `timestamptz` — so we stay inside the supported driver
   * type set and avoid a silent string coercion.
   */
  implicit val localDateFromTimestamptzMeta: Meta[LocalDate] =
    JavaTimeInstantMeta.timap(inst => inst.atZone(utc).toLocalDate)(d => d.atStartOfDay(utc).toInstant)

  implicit val localDateFromTimestamptzGet: Get[LocalDate] = localDateFromTimestamptzMeta.get
  implicit val localDateFromTimestamptzPut: Put[LocalDate] = localDateFromTimestamptzMeta.put

}
