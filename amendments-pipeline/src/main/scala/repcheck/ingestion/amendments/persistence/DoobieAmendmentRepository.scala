package repcheck.ingestion.amendments.persistence

import scala.concurrent.duration.FiniteDuration

import cats.data.NonEmptyList
import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.fragments.in

import repcheck.ingestion.amendments.errors.InvalidAmendmentNaturalKey
import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.amendment.AmendmentType
import repcheck.shared.models.congress.common.Chamber
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.amendment.AmendmentDO

/**
 * Doobie implementation of [[AmendmentRepository]] for `ConnectionIO`. Every SELECT lists columns explicitly in the
 * order [[AmendmentDO]] declares them — Doobie maps positionally and a stale `SELECT *` would silently shift fields the
 * next time the schema gains a column.
 *
 * Schema↔DO type bridges:
 *   - `submitted_date` is `TIMESTAMPTZ` in the schema (migration 001) but `Option[LocalDate]` in the DO. We cast on
 *     both directions: `submitted_date::date AS submitted_date` on read so the default `Get[LocalDate]` (DATE-backed)
 *     applies, and `${value}::date::timestamptz` on write to convert a `LocalDate` binding into a midnight-UTC
 *     `timestamptz` value the column accepts.
 *   - `proposed_date` and `latest_action_date` are native `DATE` columns and round-trip through the default
 *     `Get`/`Put[LocalDate]` without casting.
 *   - `amendment_type` and `chamber` are PostgreSQL enum types — `DoobieEnumInstances._` provides the
 *     `pgEnumStringOpt`-backed Get/Put instances, and INSERT bindings cast with `::amendment_type_enum` /
 *     `::chamber_type` so the prepared statement carries the right OID.
 */
class DoobieAmendmentRepository extends AmendmentRepository[ConnectionIO] {

  private val table: Fragment = Fragment.const(Tables.Amendments)

  /**
   * Explicit projection list matching [[AmendmentDO]] constructor order. The `submitted_date::date AS submitted_date`
   * cast bridges the TIMESTAMPTZ column to the `Option[LocalDate]` DO field via the default DATE-backed
   * `Get[LocalDate]`.
   */
  private val selectColumns: Fragment =
    fr"""id,
         natural_key,
         congress,
         amendment_type,
         number,
         bill_id,
         chamber,
         description,
         purpose,
         sponsor_member_id,
         submitted_date::date AS submitted_date,
         proposed_date,
         latest_action_date,
         latest_action_time,
         latest_action_text,
         update_date,
         api_url,
         parent_amendment_id,
         last_text_check_at,
         created_at,
         updated_at"""

  override def upsert(amendment: AmendmentDO): ConnectionIO[AmendmentDO] =
    sql"""
      INSERT INTO $table (
        natural_key, congress, amendment_type, number, bill_id, chamber,
        description, purpose, sponsor_member_id,
        submitted_date, proposed_date, latest_action_date, latest_action_time, latest_action_text,
        update_date, api_url, parent_amendment_id, last_text_check_at
      ) VALUES (
        ${amendment.naturalKey},
        ${amendment.congress},
        ${amendment.amendmentType},
        ${amendment.number},
        ${amendment.billId},
        ${amendment.chamber},
        ${amendment.description},
        ${amendment.purpose},
        ${amendment.sponsorMemberId},
        ${amendment.submittedDate}::date::timestamptz,
        ${amendment.proposedDate},
        ${amendment.latestActionDate},
        ${amendment.latestActionTime},
        ${amendment.latestActionText},
        ${amendment.updateDate},
        ${amendment.apiUrl},
        ${amendment.parentAmendmentId},
        ${amendment.lastTextCheckAt}
      )
      ON CONFLICT (natural_key) DO UPDATE SET
        congress = EXCLUDED.congress,
        amendment_type = EXCLUDED.amendment_type,
        number = EXCLUDED.number,
        bill_id = EXCLUDED.bill_id,
        chamber = EXCLUDED.chamber,
        description = EXCLUDED.description,
        purpose = EXCLUDED.purpose,
        sponsor_member_id = EXCLUDED.sponsor_member_id,
        submitted_date = EXCLUDED.submitted_date,
        proposed_date = EXCLUDED.proposed_date,
        latest_action_date = EXCLUDED.latest_action_date,
        latest_action_time = EXCLUDED.latest_action_time,
        latest_action_text = EXCLUDED.latest_action_text,
        update_date = EXCLUDED.update_date,
        api_url = EXCLUDED.api_url,
        parent_amendment_id = EXCLUDED.parent_amendment_id,
        last_text_check_at = EXCLUDED.last_text_check_at,
        updated_at = NOW()
      RETURNING
        id,
        natural_key,
        congress,
        amendment_type,
        number,
        bill_id,
        chamber,
        description,
        purpose,
        sponsor_member_id,
        submitted_date::date AS submitted_date,
        proposed_date,
        latest_action_date,
        latest_action_time,
        latest_action_text,
        update_date,
        api_url,
        parent_amendment_id,
        last_text_check_at,
        created_at,
        updated_at
    """.query[AmendmentDO].unique

  override def upsertPlaceholder(naturalKey: String): ConnectionIO[Unit] =
    parsePlaceholderNaturalKey(naturalKey) match {
      case Right((congress, amendmentType, number)) =>
        val chamber = chamberFromAmendmentType(amendmentType)
        // Single statement per call — `ON CONFLICT (natural_key) DO NOTHING` makes repeated calls idempotent.
        // Mirrors the BillRepository.upsertPlaceholder precedent: writers reserve a FK target with the minimum
        // viable row, and the owning pipeline overwrites with the real row via [[upsert]] on its next pass.
        sql"""INSERT INTO $table (natural_key, congress, amendment_type, number, chamber)
              VALUES ($naturalKey, $congress, $amendmentType, $number, $chamber)
              ON CONFLICT (natural_key) DO NOTHING""".update.run.void

      case Left(err) =>
        doobie.free.connection.raiseError[Unit](err)
    }

  override def insertIfNotExists(entity: AmendmentDO): ConnectionIO[Unit] =
    sql"""
      INSERT INTO $table (
        natural_key, congress, amendment_type, number, bill_id, chamber,
        description, purpose, sponsor_member_id,
        submitted_date, proposed_date, latest_action_date, latest_action_time, latest_action_text,
        update_date, api_url, parent_amendment_id, last_text_check_at
      ) VALUES (
        ${entity.naturalKey},
        ${entity.congress},
        ${entity.amendmentType},
        ${entity.number},
        ${entity.billId},
        ${entity.chamber},
        ${entity.description},
        ${entity.purpose},
        ${entity.sponsorMemberId},
        ${entity.submittedDate}::date::timestamptz,
        ${entity.proposedDate},
        ${entity.latestActionDate},
        ${entity.latestActionTime},
        ${entity.latestActionText},
        ${entity.updateDate},
        ${entity.apiUrl},
        ${entity.parentAmendmentId},
        ${entity.lastTextCheckAt}
      )
      ON CONFLICT (natural_key) DO NOTHING
    """.update.run.void

  override def findById(id: Long): ConnectionIO[Option[AmendmentDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE id = $id")
      .query[AmendmentDO]
      .option

  override def findByNaturalKey(naturalKey: String): ConnectionIO[Option[AmendmentDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE natural_key = $naturalKey")
      .query[AmendmentDO]
      .option

  override def findByNaturalKeys(naturalKeys: List[String]): ConnectionIO[Map[String, AmendmentDO]] =
    NonEmptyList.fromList(naturalKeys) match {
      case None =>
        // Empty input must short-circuit without issuing SQL — invariants assertion in the spec.
        doobie.free.connection.pure(Map.empty[String, AmendmentDO])
      case Some(nel) =>
        (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE" ++ in(fr"natural_key", nel))
          .query[AmendmentDO]
          .to[List]
          .map(_.iterator.map(a => a.naturalKey -> a).toMap)
    }

  override def findByBillId(billId: Long): ConnectionIO[List[AmendmentDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE bill_id = $billId ORDER BY id")
      .query[AmendmentDO]
      .to[List]

  override def findByCongress(congress: Int): ConnectionIO[List[AmendmentDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE congress = $congress ORDER BY natural_key")
      .query[AmendmentDO]
      .to[List]

  override def findByParentAmendmentId(parentId: Long): ConnectionIO[List[AmendmentDO]] =
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++ fr"WHERE parent_amendment_id = $parentId ORDER BY id")
      .query[AmendmentDO]
      .to[List]

  override def findCandidatesForTextCheck(
    minCongress: Int,
    staleAfter: FiniteDuration,
  ): ConnectionIO[List[AmendmentDO]] = {
    // Pre-filter for the amendment-text scheduler:
    //   - `congress >= minCongress` keeps the sweep within the digitized-text era operators care about.
    //   - `amendment_type != SUAMDT` excludes Senate sub-amendments — Congress.gov stores text only on the
    //     parent amendment for SUAMDTs, so the scheduler must skip them here rather than burning a request
    //     budget on a guaranteed empty response downstream.
    //   - The `last_text_check_at` clause selects rows that have either never been checked (NULL) or whose
    //     last successful check is older than the stale window (driven by Postgres' clock so the comparison
    //     stays consistent with the `NOW()` write done by [[updateLastTextCheckAt]]).
    val staleSeconds = staleAfter.toSeconds
    val staleInterval =
      Fragment.const(s"INTERVAL '${staleSeconds.toString} seconds'")
    (fr"SELECT" ++ selectColumns ++ fr"FROM" ++ table ++
      fr"WHERE congress >= $minCongress" ++
      fr"AND amendment_type <> 'suamdt'::amendment_type_enum" ++
      fr"AND (last_text_check_at IS NULL OR last_text_check_at < NOW() -" ++ staleInterval ++ fr")" ++
      fr"ORDER BY id")
      .query[AmendmentDO]
      .to[List]
  }

  override def updateLastTextCheckAt(amendmentId: Long): ConnectionIO[Unit] =
    sql"""UPDATE $table SET last_text_check_at = NOW() WHERE id = $amendmentId""".update.run.void

  /**
   * Parse a `{congress}-{AMENDMENT_TYPE}-{number}` natural key into its triple. Uses [[AmendmentType.fromString]]
   * (case-insensitive) so callers may pass either the canonical upper-case form (e.g. `"117-SAMDT-2137"`) or the
   * lower-case API value (`"117-samdt-2137"`).
   */
  private[persistence] def parsePlaceholderNaturalKey(
    naturalKey: String
  ): Either[InvalidAmendmentNaturalKey, (Int, AmendmentType, String)] = {
    val parts = naturalKey.split("-", 3)
    if (parts.length != 3) {
      Left(
        InvalidAmendmentNaturalKey(
          naturalKey,
          s"natural key must have 3 '-' segments; got ${parts.length.toString}",
        )
      )
    } else {
      for {
        congress <- parts(0).toIntOption.toRight(
          InvalidAmendmentNaturalKey(naturalKey, s"congress segment '${parts(0)}' is not an int")
        )
        amendmentType <- AmendmentType
          .fromString(parts(1))
          .left
          .map(e => InvalidAmendmentNaturalKey(naturalKey, e.getMessage))
        number <- {
          val candidate = parts(2)
          if (candidate.nonEmpty) Right(candidate)
          else
            Left(InvalidAmendmentNaturalKey(naturalKey, "number segment is empty"))
        }
      } yield (congress, amendmentType, number)
    }
  }

  /**
   * Derive `chamber` from `amendmentType`. HAMDT is always House; SAMDT and SUAMDT are always Senate. Used by
   * [[upsertPlaceholder]] because the `amendments.chamber` column is `NOT NULL` (migration 038) and a placeholder
   * carries no DTO field to read it from.
   */
  private[persistence] def chamberFromAmendmentType(amendmentType: AmendmentType): Chamber =
    amendmentType match {
      case AmendmentType.HAMDT                        => Chamber.House
      case AmendmentType.SAMDT | AmendmentType.SUAMDT => Chamber.Senate
    }

}
