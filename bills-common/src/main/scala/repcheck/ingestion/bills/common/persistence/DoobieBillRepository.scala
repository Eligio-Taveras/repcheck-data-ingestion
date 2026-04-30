package repcheck.ingestion.bills.common.persistence

import cats.data.NonEmptyList
import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.ingestion.bills.common.errors.InvalidBillNaturalKey
import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.dos.bill.BillDO

// floatArrayGet/floatArrayPut no longer needed — text_embedding removed from bills table

class DoobieBillRepository extends BillRepository[ConnectionIO] {

  private val table = Fragment.const(Tables.Bills)

  private val selectColumns: Fragment = fr"""
    id,
    (congress::text || '-' || UPPER(bill_type::text) || '-' || number::text),
    congress,
    bill_type,
    number::text,
    title,
    origin_chamber,
    origin_chamber_code,
    introduced_date,
    policy_area,
    latest_action_date,
    latest_action_text,
    constitutional_authority_text,
    sponsor_member_id,
    text_url,
    text_format,
    text_version_type,
    text_date::date,
    text_content,
    summary_text,
    summary_action_desc,
    summary_action_date,
    update_date,
    update_date_including_text,
    legislation_url,
    api_url,
    created_at,
    updated_at,
    latest_text_version_id
  """

  override def upsert(bill: BillDO): ConnectionIO[Long] =
    sql"""
      INSERT INTO $table (
        congress, bill_type, number, title,
        origin_chamber, origin_chamber_code,
        introduced_date, policy_area,
        latest_action_date, latest_action_text,
        constitutional_authority_text, sponsor_member_id,
        text_url, text_format, text_version_type, text_date,
        text_content,
        summary_text, summary_action_desc, summary_action_date,
        update_date, update_date_including_text,
        legislation_url, api_url
      ) VALUES (
        ${bill.congress}, ${bill.billType}, ${bill.number}::int, ${bill.title},
        ${bill.originChamber}, ${bill.originChamberCode}::origin_chamber_code_type,
        ${bill.introducedDate}, ${bill.policyArea},
        ${bill.latestActionDate}, ${bill.latestActionText},
        ${bill.constitutionalAuthorityText}, ${bill.sponsorMemberId},
        ${bill.textUrl}, ${bill.textFormat}, ${bill.textVersionType},
        ${bill.textDate},
        ${bill.textContent},
        ${bill.summaryText}, ${bill.summaryActionDesc},
        ${bill.summaryActionDate},
        ${bill.updateDate}, ${bill.updateDateIncludingText},
        ${bill.legislationUrl}, ${bill.apiUrl}
      )
      ON CONFLICT (congress, bill_type, number) DO UPDATE SET
        title = EXCLUDED.title,
        origin_chamber = EXCLUDED.origin_chamber,
        origin_chamber_code = EXCLUDED.origin_chamber_code,
        introduced_date = EXCLUDED.introduced_date,
        policy_area = EXCLUDED.policy_area,
        latest_action_date = EXCLUDED.latest_action_date,
        latest_action_text = EXCLUDED.latest_action_text,
        constitutional_authority_text = EXCLUDED.constitutional_authority_text,
        sponsor_member_id = EXCLUDED.sponsor_member_id,
        text_url = EXCLUDED.text_url,
        text_format = EXCLUDED.text_format,
        text_version_type = EXCLUDED.text_version_type,
        text_date = EXCLUDED.text_date,
        text_content = EXCLUDED.text_content,

        summary_text = EXCLUDED.summary_text,
        summary_action_desc = EXCLUDED.summary_action_desc,
        summary_action_date = EXCLUDED.summary_action_date,
        update_date = EXCLUDED.update_date,
        update_date_including_text = EXCLUDED.update_date_including_text,
        legislation_url = EXCLUDED.legislation_url,
        api_url = EXCLUDED.api_url,
        updated_at = NOW()
      RETURNING id
    """.query[Long].unique

  override def findByBillId(billId: String): ConnectionIO[Option[BillDO]] = {
    val (congress, billType, number) = parseNaturalKey(billId)
    (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE congress = $congress AND bill_type::text = $billType AND number = $number::int")
      .query[BillDO]
      .option
  }

  override def findByBillIds(billIds: List[String]): ConnectionIO[List[BillDO]] =
    if (billIds.isEmpty) {
      doobie.free.connection.pure(List.empty[BillDO])
    } else {
      val parsed    = billIds.map(parseNaturalKey)
      val fragments = parsed.map { case (c, t, n) => fr"(congress = $c AND bill_type::text = $t AND number = $n::int)" }
      val conditions = fragments match {
        case first :: rest => rest.foldLeft(first)(_ ++ fr" OR " ++ _)
        case Nil           => fr"FALSE"
      }
      (fr"SELECT" ++ selectColumns ++ fr"FROM $table WHERE" ++ conditions)
        .query[BillDO]
        .to[List]
    }

  override def findBillsNeedingTextCheck(congresses: List[Int]): ConnectionIO[List[BillDO]] = {
    // Stage-aware filter. Returns bills whose stored stage doesn't match the expected stage, ordered
    // so genuinely pending bills (no text downloaded yet) come before "upgrade-bucket" bills (text
    // downloaded but at a different version code).
    //
    //   * `expected_text_version_code IS NOT NULL` — fail-safe: bills with no stage signal yet stay
    //     out of the sweep.
    //   * `text_version_type IS DISTINCT FROM expected_text_version_code` — handles both "stored is
    //     NULL" and "stored != expected" with one operator.
    //   * `congress = ANY($congresses)` — operator-controlled allow-list. Pre-103 bills have
    //     `expected_text_version_code` populated by the metadata pipeline's chamber-floor write,
    //     but Congress.gov has no text body for them. Empty list = no congress filter (sweep all).
    //     Default in `BillTextCheckerConfig.congresses` is "103,104,...,119".
    //
    // ORDER BY (latest_text_version_id IS NULL) DESC: prioritize never-downloaded bills over upgrade-
    // bucket bills. The upgrade bucket can otherwise starve the pending work — a single per-run scan
    // is rate-limited by Congress.gov, so processing "Text unchanged" no-ops first leaves no budget
    // for the actual backfill.
    val congressFilter = congresses match {
      case Nil          => Fragment.empty
      case head :: tail => fr"AND" ++ Fragments.in(fr"congress", NonEmptyList(head, tail))
    }
    (fr"SELECT" ++ selectColumns ++ fr"FROM $table" ++
      fr"WHERE expected_text_version_code IS NOT NULL" ++
      fr"AND text_version_type IS DISTINCT FROM expected_text_version_code" ++
      congressFilter ++
      fr"ORDER BY (latest_text_version_id IS NULL) DESC, congress DESC, id ASC")
      .query[BillDO]
      .to[List]
  }

  override def updateTextFields(
    billId: String,
    textUrl: String,
    textFormat: String,
    textVersionType: String,
    textDate: String,
    latestTextVersionId: Long,
  ): ConnectionIO[Unit] = {
    val (congress, billType, number) = parseNaturalKey(billId)
    val updateText: ConnectionIO[Unit] = sql"""
      UPDATE $table SET
        text_url = $textUrl,
        text_format = $textFormat::format_type_enum,
        text_version_type = $textVersionType::text_version_code_type,
        text_date = $textDate::timestamptz,
        latest_text_version_id = $latestTextVersionId,
        updated_at = NOW()
      WHERE congress = $congress AND bill_type::text = $billType AND number = $number::int
    """.update.run.void

    // Cooperative-write contract repair: when a successfully-downloaded text version's progressionOrder
    // exceeds the current `expected_text_version_code`, bump expected to match. Without this, bills can
    // end up stuck in `findBillsNeedingTextCheck`'s `IS DISTINCT FROM` filter forever — the metadata or
    // summary pipeline set expected to some early stage (e.g. IH), bill-text-pipeline downloaded a later
    // stage (e.g. ATS), the SQL filter keeps surfacing the bill, the runtime check decides "Text
    // unchanged" because Congress.gov's latest matches what we have. Self-heals here so freshly-
    // downloaded rows escape the loop on the next text-pipeline pass; new rows never enter the broken
    // state.
    val maybeBumpExpected: ConnectionIO[Unit] = TextVersionCode.fromString(textVersionType) match {
      case Left(_) =>
        // textVersionType doesn't parse to a known TextVersionCode. The Postgres enum cast above
        // will fail anyway, so the whole transaction aborts; nothing to do here.
        doobie.free.connection.unit
      case Right(downloaded) =>
        findExpectedVersion(billId).flatMap { existing =>
          val shouldBump = existing match {
            case None          => true
            case Some(current) => downloaded.progressionOrder > current.progressionOrder
          }
          if (shouldBump) updateExpectedVersion(billId, downloaded)
          else doobie.free.connection.unit
        }
    }

    updateText *> maybeBumpExpected
  }

  private[persistence] def parseNaturalKey(naturalKey: String): (Int, String, String) = {
    val parts = naturalKey.split("-", 3)
    (parts(0).toInt, parts(1).toLowerCase, parts(2))
  }

  /**
   * Safe variant of [[parseNaturalKey]] returning an [[InvalidBillNaturalKey]] on any parse failure instead of throwing
   * a `NumberFormatException` / `ArrayIndexOutOfBoundsException`. Used by [[upsertPlaceholder]] below. Does NOT
   * lowercase the bill-type segment — instead it resolves it through [[BillType.fromString]] (which is
   * case-insensitive) so the caller can bind a typed `BillType` via Doobie's `Put[BillType]`.
   */
  private[persistence] def parsePlaceholderNaturalKey(
    naturalKey: String
  ): Either[InvalidBillNaturalKey, (Int, BillType, Int)] = {
    val parts = naturalKey.split("-", 3)
    if (parts.length != 3) {
      Left(
        InvalidBillNaturalKey(naturalKey, s"natural key must have 3 '-' segments; got ${parts.length.toString}")
      )
    } else {
      for {
        congress <- parts(0).toIntOption.toRight(
          InvalidBillNaturalKey(naturalKey, s"congress segment '${parts(0)}' is not an int")
        )
        billType <- BillType.fromString(parts(1)).left.map(e => InvalidBillNaturalKey(naturalKey, e.getMessage))
        number <- parts(2).toIntOption.toRight(
          InvalidBillNaturalKey(naturalKey, s"number segment '${parts(2)}' is not an int")
        )
      } yield (congress, billType, number)
    }
  }

  override def upsertPlaceholder(naturalKey: String): ConnectionIO[Unit] =
    parsePlaceholderNaturalKey(naturalKey) match {
      case Right((congress, billType, number)) =>
        // `title` NOT NULL on bills → empty string stub; bills-pipeline overwrites it on enrichment.
        // `update_date` NOT NULL → NOW() stub; also overwritten on enrichment.
        sql"""INSERT INTO $table (congress, bill_type, number, title, update_date)
              VALUES ($congress, $billType, $number, '', NOW())
              ON CONFLICT (congress, bill_type, number) DO NOTHING""".update.run.void

      case Left(err) =>
        doobie.free.connection.raiseError[Unit](err)
    }

  override def findExpectedVersion(naturalKey: String): ConnectionIO[Option[TextVersionCode]] = {
    val (congress, billType, number) = parseNaturalKey(naturalKey)
    sql"""SELECT expected_text_version_code
          FROM $table
          WHERE congress = $congress AND bill_type::text = $billType AND number = $number::int"""
      .query[Option[TextVersionCode]]
      .option
      .map(_.flatten)
  }

  override def updateExpectedVersion(naturalKey: String, code: TextVersionCode): ConnectionIO[Unit] = {
    val (congress, billType, number) = parseNaturalKey(naturalKey)
    sql"""UPDATE $table SET
            expected_text_version_code = $code,
            updated_at = NOW()
          WHERE congress = $congress AND bill_type::text = $billType AND number = $number::int""".update.run.void
  }

}
