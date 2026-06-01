package repcheck.ingestion.bills.common.persistence

import java.time.{Instant, LocalDate}

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.testing.{DockerRequired, TransactorFixture}
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.common.DoobieEnumInstances._
import repcheck.shared.models.congress.common.{BillType, Chamber, FormatType}
import repcheck.shared.models.congress.dos.bill.{BillDO, BillTextVersionDO}

class DoobieBillRepositorySpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo            = new DoobieBillRepository
  private lazy val textVersionRepo = new DoobieBillTextVersionRepository

  /**
   * Insert a text version row and return its auto-generated id. `versionCode` must match the stage the
   * caller then writes via updateTextFields — Phase 2c reads the stored stage from this row (via
   * latest_text_version_id), not the bills.text_version_type column.
   */
  private def insertTextVersion(billId: Long, versionCode: String = "IH"): Long =
    textVersionRepo
      .insertVersion(
        BillTextVersionDO(
          id = 0L,
          billId = billId,
          versionCode = versionCode,
          versionType = s"$versionCode version",
          versionDate = Some(LocalDate.parse("2024-01-15")),
          formatType = Some(FormatType.FormattedText),
          url = Some("https://congress.gov/text/IH"),
          fetchedAt = Some(Instant.now()),
          createdAt = None,
        )
      )
      .transact(xa)
      .unsafeRunSync()

  private def makeBill(
    congress: Int = 118,
    billType: BillType = BillType.HR,
    number: String = "1234",
    title: String = "Test Bill",
    updateDate: Option[Instant] = Some(Instant.parse("2024-01-15T00:00:00Z")),
  ): BillDO = BillDO(
    billId = 0L,
    naturalKey = s"$congress-${billType.toString}-$number",
    congress = congress,
    billType = billType,
    number = number,
    title = title,
    originChamber = Some(Chamber.House),
    originChamberCode = Some("H"),
    introducedDate = Some(LocalDate.parse("2024-01-10")),
    policyArea = Some("Government Operations"),
    latestActionDate = Some(LocalDate.parse("2024-03-15")),
    latestActionText = Some("Referred to committee"),
    constitutionalAuthorityText = Some("Article I"),
    sponsorMemberId = None,
    textUrl = None,
    textFormat = None,
    textVersionType = None,
    textDate = None,
    textContent = None,
    summaryText = None,
    summaryActionDesc = None,
    summaryActionDate = None,
    updateDate = updateDate,
    updateDateIncludingText = None,
    legislationUrl = Some("https://congress.gov/bill/118th-congress/house-bill/1234"),
    apiUrl = Some("https://api.congress.gov/v3/bill/118/hr/1234"),
    createdAt = None,
    updatedAt = None,
    latestTextVersionId = None,
  )

  "upsert" should "insert a new bill" taggedAs DockerRequired in {
    val bill = makeBill()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    val _     = found shouldBe defined
    found match {
      case Some(bill) =>
        val _ = bill.title shouldBe "Test Bill"
        val _ = bill.congress shouldBe 118
        val _ = bill.billType shouldBe BillType.HR
        bill.naturalKey shouldBe "118-HR-1234"
      case None => fail("Expected bill to be present")
    }
  }

  it should "preserve text_version_type and summary_text on a metadata re-upsert (ownership boundary)" taggedAs DockerRequired in {
    val bill = makeBill(number = "9100")
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    // Simulate the bill-text and bill-summary pipelines writing the columns they own.
    val _ = sql"""UPDATE bills
                  SET text_version_type = 'EH'::text_version_code_type, summary_text = 'CRS summary body'
                  WHERE congress = 118 AND bill_type = 'hr'::bill_type_enum AND number = 9100""".update.run
      .transact(xa)
      .unsafeRunSync()

    // The metadata pipeline re-upserts the same bill; its DTO carries None for both (the bill detail API has neither).
    // After the ownership-boundary fix the UPDATE SET no longer lists those columns, so they must survive.
    val _ = repo.upsert(bill.copy(title = "Updated Title")).transact(xa).unsafeRunSync()

    val _ = repo.findByBillId("118-HR-9100").transact(xa).unsafeRunSync() match {
      case Some(b) =>
        val _ = b.title shouldBe "Updated Title"        // metadata-owned column DID update
        b.summaryText shouldBe Some("CRS summary body") // summary-owned column NOT clobbered to NULL
      case None => fail("Expected bill to be present")
    }
    // Assert text_version_type on the raw column: Phase 2c sources BillDO.textVersionType from
    // bill_text_versions, but this test verifies the bills column itself survived the metadata re-upsert.
    val storedVersion = sql"""SELECT text_version_type::text FROM bills
                              WHERE congress = 118 AND bill_type = 'hr'::bill_type_enum AND number = 9100"""
      .query[Option[String]]
      .unique
      .transact(xa)
      .unsafeRunSync()
    storedVersion shouldBe Some("EH")
  }

  it should "return the generated bill id" taggedAs DockerRequired in {
    val bill       = makeBill()
    val returnedId = repo.upsert(bill).transact(xa).unsafeRunSync()
    returnedId should be > 0L
  }

  it should "update an existing bill on conflict" taggedAs DockerRequired in {
    val bill = makeBill()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val updated = makeBill(title = "Updated Title")
    val _       = repo.upsert(updated).transact(xa).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    found.map(_.title) shouldBe Some("Updated Title")
  }

  it should "not create duplicate rows on conflict" taggedAs DockerRequired in {
    val bill = makeBill()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val all = repo.findByBillIds(List("118-HR-1234")).transact(xa).unsafeRunSync()
    all.size shouldBe 1
  }

  it should "set updated_at on conflict" taggedAs DockerRequired in {
    val bill  = makeBill()
    val _     = repo.upsert(bill).transact(xa).unsafeRunSync()
    val first = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()

    Thread.sleep(50)
    val _      = repo.upsert(makeBill(title = "Changed")).transact(xa).unsafeRunSync()
    val second = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()

    second.flatMap(_.updatedAt) should not be first.flatMap(_.updatedAt)
  }

  "findByBillId" should "return None for missing bill" taggedAs DockerRequired in {
    val found = repo.findByBillId("118-HR-9999").transact(xa).unsafeRunSync()
    found shouldBe None
  }

  it should "return stored bill with all fields" taggedAs DockerRequired in {
    val bill = makeBill()
    val _    = repo.upsert(bill).transact(xa).unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    found match {
      case Some(bill) =>
        val _ = bill.originChamber shouldBe Some(Chamber.House)
        val _ = bill.policyArea shouldBe Some("Government Operations")
        bill.legislationUrl shouldBe Some("https://congress.gov/bill/118th-congress/house-bill/1234")
      case None => fail("Expected bill to be present")
    }
  }

  "findByBillIds" should "return matching bills" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill(number = "1")).transact(xa).unsafeRunSync()
    val _ = repo.upsert(makeBill(number = "2")).transact(xa).unsafeRunSync()
    val _ = repo.upsert(makeBill(number = "3")).transact(xa).unsafeRunSync()

    val found = repo.findByBillIds(List("118-HR-1", "118-HR-2")).transact(xa).unsafeRunSync()
    found.size shouldBe 2
  }

  it should "ignore missing IDs" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill(number = "1")).transact(xa).unsafeRunSync()

    val found = repo.findByBillIds(List("118-HR-1", "118-HR-9999")).transact(xa).unsafeRunSync()
    found.size shouldBe 1
  }

  it should "return empty for empty input" taggedAs DockerRequired in {
    val found = repo.findByBillIds(List.empty).transact(xa).unsafeRunSync()
    found shouldBe empty
  }

  "findBillsNeedingTextCheck" should "exclude bills with expected_text_version_code IS NULL (no stage signal yet)" taggedAs DockerRequired in {
    // Post-migration-032 filter is `WHERE expected_text_version_code IS NOT NULL AND
    // text_version_type IS DISTINCT FROM expected_text_version_code`. A bill with NULL expected
    // hasn't been touched by either bill-metadata-pipeline (introduced floor) or bill-summary-pipeline
    // (advancing writer) yet — fail-safe: don't sweep it.
    val _     = repo.upsert(makeBill(number = "1")).transact(xa).unsafeRunSync()
    val found = repo.findBillsNeedingTextCheck(List.empty).transact(xa).unsafeRunSync()
    found.map(_.naturalKey) should not contain "118-HR-1"
  }

  it should "include bills where expected_text_version_code is set and text_version_type is NULL" taggedAs DockerRequired in {
    // Most common case: bill-metadata-pipeline floored the bill to IH/IS, bill-text-pipeline
    // hasn't downloaded yet → text_version_type is NULL → IS DISTINCT FROM IH is true → sweep.
    val _ = repo.upsert(makeBill(number = "10")).transact(xa).unsafeRunSync()
    val _ = repo.updateExpectedVersion("118-HR-10", TextVersionCode.IH).transact(xa).unsafeRunSync()

    val found = repo.findBillsNeedingTextCheck(List.empty).transact(xa).unsafeRunSync()
    found.map(_.naturalKey) should contain("118-HR-10")
  }

  it should "exclude bills where text_version_type matches expected_text_version_code" taggedAs DockerRequired in {
    // Steady state: bill-text-pipeline downloaded the matching version and bumped text_version_type.
    // expected = stored → IS DISTINCT FROM is false → bill exits the sweep.
    val _ = repo.upsert(makeBill(number = "20")).transact(xa).unsafeRunSync()
    val billId =
      repo.findByBillId("118-HR-20").transact(xa).unsafeRunSync().map(_.billId).getOrElse(sys.error("missing"))
    val versionId = insertTextVersion(billId)

    val _ = repo.updateExpectedVersion("118-HR-20", TextVersionCode.IH).transact(xa).unsafeRunSync()
    val _ = repo
      .updateTextFields("118-HR-20", "http://text", "Formatted Text", "IH", "2024-01-15T00:00:00Z", versionId)
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findBillsNeedingTextCheck(List.empty).transact(xa).unsafeRunSync()
    found.map(_.naturalKey) should not contain "118-HR-20"
  }

  it should "include bills where expected has advanced past stored (re-version detection)" taggedAs DockerRequired in {
    // The whole point of the stage-aware filter: bill-summary-pipeline advanced expected from IH
    // to RH, but bill-text-pipeline still has IH stored. The bill must enter the sweep so
    // bill-text-availability-checker can fetch the new RH version.
    val _ = repo.upsert(makeBill(number = "30")).transact(xa).unsafeRunSync()
    val billId =
      repo.findByBillId("118-HR-30").transact(xa).unsafeRunSync().map(_.billId).getOrElse(sys.error("missing"))
    val versionId = insertTextVersion(billId)

    val _ = repo.updateExpectedVersion("118-HR-30", TextVersionCode.RH).transact(xa).unsafeRunSync()
    val _ = repo
      .updateTextFields("118-HR-30", "http://text", "Formatted Text", "IH", "2024-01-15T00:00:00Z", versionId)
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findBillsNeedingTextCheck(List.empty).transact(xa).unsafeRunSync()
    found.map(_.naturalKey) should contain("118-HR-30")
  }

  it should "exclude terminal-stage bills (PL stored matches PL expected)" taggedAs DockerRequired in {
    // Bills that became law: text_version_type = PL, expected = PL → permanent exit from the sweep.
    val _ = repo.upsert(makeBill(number = "40")).transact(xa).unsafeRunSync()
    val billId =
      repo.findByBillId("118-HR-40").transact(xa).unsafeRunSync().map(_.billId).getOrElse(sys.error("missing"))
    val versionId = insertTextVersion(billId, "PL")

    val _ = repo.updateExpectedVersion("118-HR-40", TextVersionCode.PL).transact(xa).unsafeRunSync()
    val _ = repo
      .updateTextFields("118-HR-40", "http://text", "Formatted Text", "PL", "2024-06-01T00:00:00Z", versionId)
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findBillsNeedingTextCheck(List.empty).transact(xa).unsafeRunSync()
    found.map(_.naturalKey) should not contain "118-HR-40"
  }

  "findExpectedVersion" should "return None for bills not yet in the table" taggedAs DockerRequired in {
    repo.findExpectedVersion("118-HR-9999").transact(xa).unsafeRunSync() shouldBe None
  }

  it should "return None for bills with NULL expected_text_version_code (default after upsert)" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill(number = "50")).transact(xa).unsafeRunSync()
    repo.findExpectedVersion("118-HR-50").transact(xa).unsafeRunSync() shouldBe None
  }

  it should "round-trip every TextVersionCode via the typed enum mapping" taggedAs DockerRequired in {
    // Sanity check that the Doobie Get[TextVersionCode] resolves the column correctly. Sample a few
    // codes spanning the progressionOrder tiers so we'd catch a mismatch on any of them.
    val cases = List(
      "60" -> TextVersionCode.IH,
      "61" -> TextVersionCode.RH,
      "62" -> TextVersionCode.EH,
      "63" -> TextVersionCode.EAH,
      "64" -> TextVersionCode.ENR,
      "65" -> TextVersionCode.PL,
    )
    cases.foreach {
      case (number, code) =>
        val _    = repo.upsert(makeBill(number = number)).transact(xa).unsafeRunSync()
        val _    = repo.updateExpectedVersion(s"118-HR-$number", code).transact(xa).unsafeRunSync()
        val read = repo.findExpectedVersion(s"118-HR-$number").transact(xa).unsafeRunSync()
        val _    = read shouldBe Some(code)
    }
  }

  "updateExpectedVersion" should "set the column on an existing bill" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill(number = "70")).transact(xa).unsafeRunSync()
    val _ = repo.updateExpectedVersion("118-HR-70", TextVersionCode.RH).transact(xa).unsafeRunSync()
    repo.findExpectedVersion("118-HR-70").transact(xa).unsafeRunSync() shouldBe Some(TextVersionCode.RH)
  }

  it should "be unconditional: it does not enforce the progressionOrder regression guard at the SQL level" taggedAs DockerRequired in {
    // The regression guard lives at the caller (Scala read-then-write). The SQL UPDATE writes
    // whatever it's told. This test guards against a future "helpful" SQL-level WHERE clause
    // sneaking in that would silently swallow caller mistakes.
    val _ = repo.upsert(makeBill(number = "80")).transact(xa).unsafeRunSync()
    val _ = repo.updateExpectedVersion("118-HR-80", TextVersionCode.PL).transact(xa).unsafeRunSync()
    val _ = repo.updateExpectedVersion("118-HR-80", TextVersionCode.IH).transact(xa).unsafeRunSync()
    repo.findExpectedVersion("118-HR-80").transact(xa).unsafeRunSync() shouldBe Some(TextVersionCode.IH)
  }

  it should "bump updated_at on the bills row" taggedAs DockerRequired in {
    val _     = repo.upsert(makeBill(number = "90")).transact(xa).unsafeRunSync()
    val first = repo.findByBillId("118-HR-90").transact(xa).unsafeRunSync().flatMap(_.updatedAt)

    Thread.sleep(50)
    val _      = repo.updateExpectedVersion("118-HR-90", TextVersionCode.IH).transact(xa).unsafeRunSync()
    val second = repo.findByBillId("118-HR-90").transact(xa).unsafeRunSync().flatMap(_.updatedAt)

    second should not be first
  }

  "updateTextFields" should "set text columns without touching metadata" taggedAs DockerRequired in {
    val _ = repo.upsert(makeBill()).transact(xa).unsafeRunSync()
    val billId =
      repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync().map(_.billId).getOrElse(sys.error("missing"))
    val versionId = insertTextVersion(billId, "RH")

    repo
      .updateTextFields("118-HR-1234", "http://text.xml", "Formatted XML", "RH", "2024-02-01T00:00:00Z", versionId)
      .transact(xa)
      .unsafeRunSync()

    val found = repo.findByBillId("118-HR-1234").transact(xa).unsafeRunSync()
    found match {
      case Some(bill) =>
        val _ = bill.title shouldBe "Test Bill"
        val _ = bill.textUrl shouldBe Some("http://text.xml")
        val _ = bill.textFormat shouldBe Some(FormatType.FormattedXml)
        val _ = bill.textVersionType shouldBe Some(TextVersionCode.RH)
        bill.latestTextVersionId shouldBe Some(versionId)
      case None => fail("Expected bill to be present")
    }
  }

  // ==========================================================================================
  // upsertPlaceholder — used by the votes pipeline to reserve a FK target before bills-pipeline
  // enriches the real row. Verifies the SQL shape + composite-key idempotency against a real
  // Postgres instance.
  // ==========================================================================================

  private def countBillsByNaturalKey(congress: Int, billType: BillType, number: Int): Int =
    sql"""SELECT COUNT(*) FROM bills
          WHERE congress = $congress AND bill_type = $billType AND number = $number"""
      .query[Int]
      .unique
      .transact(xa)
      .unsafeRunSync()

  "upsertPlaceholder" should "insert a new bills row with the parsed composite key" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("119-HR-30").transact(xa).unsafeRunSync()
    val _ = countBillsByNaturalKey(119, BillType.HR, 30) shouldBe 1

    val stored = repo.findByBillId("119-HR-30").transact(xa).unsafeRunSync()
    stored match {
      case Some(bill) =>
        val _ = bill.congress shouldBe 119
        val _ = bill.billType shouldBe BillType.HR
        val _ = bill.number shouldBe "30"
        // Placeholder title is intentionally an empty string — bills-pipeline overwrites it on enrichment.
        bill.title shouldBe ""
      case None => fail("Expected placeholder bill to be findable via findByBillId")
    }
  }

  // CONTRACT: a row inserted via upsertPlaceholder must leave `update_date` NULL — that's the field
  // the metadata pipeline's `incoming.updateDate > stored.updateDate` comparison reads, and any non-
  // NULL value here would short-circuit a future enrichment sweep into the "Bill unchanged" branch
  // (because the writer's wall-clock NOW() is by definition newer than any real API updateDate).
  // This test was added when the historical bug — `INSERT ... VALUES (..., '', NOW())` — was fixed:
  // 161,721 placeholder rows in the local dev DB had `update_date = April 26-30, 2026` (writer
  // insertion time), and every metadata sweep skipped them silently. The fix is to leave
  // `update_date` (and `update_date_including_text`) entirely out of the INSERT so the column takes
  // its NULL default, matching the canonical `HasPlaceholder[BillDO].placeholder()` shape.
  it should "leave update_date and update_date_including_text NULL on the inserted row" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("119-HR-31").transact(xa).unsafeRunSync()

    val stored = repo.findByBillId("119-HR-31").transact(xa).unsafeRunSync()
    stored match {
      case Some(bill) =>
        val _ = bill.updateDate shouldBe None
        bill.updateDateIncludingText shouldBe None
      case None => fail("Expected placeholder bill to be findable via findByBillId")
    }
  }

  // The placeholder INSERT must produce a row that the canonical detector
  // `BillPlaceholder.isPlaceholder` recognizes. Since both this writer and the detector are pinned
  // (via `BillPlaceholderSpec`'s invariant-lock test) to the same `HasPlaceholder[BillDO]
  // .placeholder(...)` factory shape, an inadvertent override here (e.g., reintroducing a NOW()
  // stub on update_date, or accidentally populating an Option detail field) would fail this test.
  it should "produce a row matching BillPlaceholder.isPlaceholder" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("119-HR-32").transact(xa).unsafeRunSync()

    val stored = repo.findByBillId("119-HR-32").transact(xa).unsafeRunSync()
    stored match {
      case Some(bill) =>
        repcheck.ingestion.bills.common.BillPlaceholder.isPlaceholder(bill) shouldBe true
      case None => fail("Expected placeholder bill to be findable via findByBillId")
    }
  }

  it should "be idempotent when the same natural key is inserted twice (ON CONFLICT DO NOTHING)" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("119-S-42").transact(xa).unsafeRunSync()
    val _ = repo.upsertPlaceholder("119-S-42").transact(xa).unsafeRunSync()
    countBillsByNaturalKey(119, BillType.S, 42) shouldBe 1
  }

  it should "insert distinct rows for distinct natural keys" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("119-HR-30").transact(xa).unsafeRunSync()
    val _ = repo.upsertPlaceholder("119-S-42").transact(xa).unsafeRunSync()
    val _ = repo.upsertPlaceholder("119-HR-31").transact(xa).unsafeRunSync()

    val _ = countBillsByNaturalKey(119, BillType.HR, 30) shouldBe 1
    val _ = countBillsByNaturalKey(119, BillType.S, 42) shouldBe 1
    countBillsByNaturalKey(119, BillType.HR, 31) shouldBe 1
  }

  it should "NOT overwrite a real (non-placeholder) bill if one already exists at the same composite key" taggedAs DockerRequired in {
    // First persist a real bill via upsert.
    val realBill =
      makeBill(congress = 119, billType = BillType.HJRES, number = "7", title = "Real HJRES 7")
    val _ = repo.upsert(realBill).transact(xa).unsafeRunSync()

    // Then call upsertPlaceholder on the same natural key — should be a no-op.
    val _ = repo.upsertPlaceholder("119-HJRES-7").transact(xa).unsafeRunSync()

    val stored = repo.findByBillId("119-HJRES-7").transact(xa).unsafeRunSync()
    stored match {
      case Some(bill) => bill.title shouldBe "Real HJRES 7"
      case None       => fail("Expected the real bill to remain after placeholder no-op")
    }
  }

  it should "write the bill_type enum in lowercase (matches findByBillId's comparison)" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("119-HJRES-5").transact(xa).unsafeRunSync()

    // The Doobie Put[BillType] serializes BillType.HJRES to its apiValue "hjres" (lowercase) — that's
    // what the bill_type_enum PG type expects. findByBillId lowercases its parsed bill_type before
    // comparing, so writer + reader agree here.
    val rawBillType = sql"""SELECT bill_type::text FROM bills
                            WHERE congress = 119 AND number = 5"""
      .query[String]
      .unique
      .transact(xa)
      .unsafeRunSync()
    rawBillType shouldBe "hjres"
  }

}
