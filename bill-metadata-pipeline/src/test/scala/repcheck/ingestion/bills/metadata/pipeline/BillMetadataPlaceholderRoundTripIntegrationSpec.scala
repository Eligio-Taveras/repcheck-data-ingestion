package repcheck.ingestion.bills.metadata.pipeline

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.implicits._

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.BillPlaceholder
import repcheck.ingestion.bills.common.persistence.{
  DoobieBillCosponsorRepository,
  DoobieBillHistoryArchiver,
  DoobieBillRepository,
  DoobieBillSubjectRepository,
}
import repcheck.ingestion.bills.common.testing.{DockerRequired, TransactorFixture}
import repcheck.ingestion.bills.metadata.api.BillsApiClient
import repcheck.ingestion.bills.metadata.config.BillMetadataConfig
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{DoobieMemberRepository, MemberWriteInstances}
import repcheck.shared.models.congress.bill.TextVersionCode
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.{BillDetailDTO, BillListItemDTO, LatestActionDTO, SponsorDTO}
import repcheck.shared.models.congress.dto.common.PaginationInfoDTO

/**
 * Cross-pipeline placeholder round-trip integration tests against a real Postgres (AlloyDB Omni via
 * `TransactorFixture`).
 *
 * Both votes-pipeline (via `BillLookup`) and bill-summary-pipeline (via `BillSummaryProcessor.upsertPlaceholder`) route
 * through `BillRepository.upsertPlaceholder` to write a placeholder bill row when they reference a bill the metadata
 * pipeline hasn't yet ingested. The metadata pipeline is then expected to detect the placeholder on its next sweep and
 * enrich it from Congress.gov's `/bill/{c}/{type}/{n}` detail endpoint.
 *
 * Until this spec, no test wired the real writer + the real `BillMetadataProcessor` + a real DB end-to-end — which is
 * exactly why the bug we just fixed (placeholder rows poisoned with `update_date = NOW()` from a stale insert path)
 * survived for 161,721 rows. Both halves were tested in isolation; the composition wasn't. These tests pin the
 * round-trip so a future regression of either side surfaces here instead of silently breaking production.
 *
 * Coverage (as of this writing):
 *   - Test 1: votes-pipeline placeholder shape → metadata enrichment + bill_history archive
 *   - Test 2: summary-pipeline placeholder shape + advanced expected_text_version_code → metadata enrichment with the
 *     regression guard preserving the advanced stage (metadata's IH/IS floor must not regress an ENR set by summary)
 */
class BillMetadataPlaceholderRoundTripIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with TransactorFixture {

  import MemberWriteInstances._

  // Real Doobie repos against the AlloyDB Omni transactor from TransactorFixture.
  private lazy val billRepo         = new DoobieBillRepository
  private lazy val cosponsorRepo    = new DoobieBillCosponsorRepository
  private lazy val subjectRepo      = new DoobieBillSubjectRepository
  private lazy val historyArchiver  = new DoobieBillHistoryArchiver
  private lazy val memberRepo       = new DoobieMemberRepository
  private lazy val placeholder      = new DefaultPlaceholderCreator[IO]
  private lazy val memberEntityRepo = new DoobieEntityRepository[IO, MemberDO](xa, MemberInsertSql.value)

  // The processor logs heavily; we don't assert on logs here — a no-op mock keeps the test focused on
  // DB-state assertions which are the real contract.
  private def noOpLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(m.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    m
  }

  private def buildProcessor(apiClient: BillsApiClient[IO]): BillMetadataProcessor[IO] =
    new BillMetadataProcessor[IO](
      apiClient = apiClient,
      billRepo = billRepo,
      cosponsorRepo = cosponsorRepo,
      subjectRepo = subjectRepo,
      historyArchiver = historyArchiver,
      memberRepo = memberRepo,
      placeholderCreator = placeholder,
      memberEntityRepo = memberEntityRepo,
      xa = xa,
      config = BillMetadataConfig(lookbackDays = 30, parallelism = 1),
      logger = noOpLogger,
    )

  private def makeListItem(naturalKey: String): BillListItemDTO = {
    val parts = naturalKey.split('-')
    BillListItemDTO(
      congress = parts(0).toInt,
      number = parts(2),
      billType = parts(1).toLowerCase,
      originChamber = Some("House"),
      originChamberCode = Some("H"),
      title = "From-list-item title (overwritten by detail in this flow)",
      latestAction = None,
      url = s"https://api.congress.gov/v3/bill/${parts(0)}/${parts(1).toLowerCase}/${parts(2)}",
      // Pre-fix, the placeholder row's stored update_date was NOW() (newer than any real API value), so the
      // date comparison routed every placeholder to "Bill unchanged" forever. Post-fix, stored update_date
      // is NULL, and the date comparison's `storedDate.forall(...)` returns true — defense in depth alongside
      // the placeholder-shape detection. We pick a deliberately *older* updateDate here to prove the fix is
      // load-bearing: pre-fix this would skip the bill, post-fix the placeholder branch catches it before the
      // date comparison even runs.
      updateDate = Some("2020-01-15T00:00:00Z"),
      updateDateIncludingText = None,
    )
  }

  // Detail DTO with the four metadata-owned fields populated (introducedDate, latestAction's text,
  // a sponsor for sponsor_member_id resolution, and updateDate). After enrichment the resulting
  // bill row should have all four columns set so `BillPlaceholder.isPlaceholder(b)` returns false
  // and the unchanged-skip path becomes reachable on the next sweep.
  private def makeDetailDTO(naturalKey: String, title: String, updateDate: String): BillDetailDTO = {
    val parts = naturalKey.split('-')
    BillDetailDTO(
      congress = parts(0).toInt,
      number = parts(2),
      billType = parts(1).toLowerCase,
      latestAction =
        Some(LatestActionDTO(actionDate = "2025-08-01", text = "Referred to the House Committee on Rules.")),
      originChamber = Some("House"),
      originChamberCode = Some("H"),
      title = title,
      updateDate = Some(updateDate),
      updateDateIncludingText = Some(updateDate),
      url = s"https://api.congress.gov/v3/bill/${parts(0)}/${parts(1).toLowerCase}/${parts(2)}",
      introducedDate = Some("2025-07-15"),
      policyArea = None,
      sponsors = Some(
        List(
          SponsorDTO.MemberSponsorDTO(
            bioguideId = "T000001",
            firstName = Some("Test"),
            lastName = Some("Sponsor"),
            fullName = Some("Rep. Test Sponsor"),
            middleName = None,
            isByRequest = None,
            party = None,
            state = None,
            district = None,
            url = None,
          )
        )
      ),
      cosponsors = Some(PaginationInfoDTO(count = Some(0), url = None)),
      subjects = None,
      summaries = None,
      actions = None,
      committees = None,
      textVersions = None,
      titles = None,
      constitutionalAuthorityStatementText = None,
      cboCostEstimates = None,
      committeeReports = None,
      relatedBills = None,
      legislationUrl = None,
    )
  }

  private def stubApi(naturalKey: String, title: String, updateDate: String): BillsApiClient[IO] = {
    val client = mock[BillsApiClient[IO]]
    val parts  = naturalKey.split('-')
    val url    = s"https://api.congress.gov/v3/bill/${parts(0)}/${parts(1).toLowerCase}/${parts(2)}"
    when(client.fetchDetail(eqUrl(url))).thenReturn(IO.pure(makeDetailDTO(naturalKey, title, updateDate)))
    client
  }

  // Convenience wrapper around mockito's eq() for url-string matching, which is needed because the processor
  // appends `?format=json` and other tweaks to the list-item URL before calling fetchDetail.
  private def eqUrl(expected: String): String =
    org.mockito.ArgumentMatchers.argThat[String]((arg: String) => arg != null && arg.startsWith(expected))

  private def countBills(naturalKey: String): Int = {
    val parts    = naturalKey.split('-')
    val congress = parts(0).toInt
    val number   = parts(2)
    sql"""SELECT COUNT(*) FROM bills WHERE congress = $congress AND number = $number::int"""
      .query[Int]
      .unique
      .transact(xa)
      .unsafeRunSync()
  }

  // ------------------------------------------------------------------------
  // Test 1: Votes-pipeline placeholder → metadata enriches it
  // ------------------------------------------------------------------------

  "Round-trip: votes-pipeline placeholder → metadata enriches" should
    "succeed and leave a fully-populated bill row + an archived placeholder snapshot" taggedAs DockerRequired in {
      val naturalKey = "119-HR-1"
      val realTitle  = "An Act to test the round-trip placeholder enrichment flow"
      val realUpdate = "2025-08-15T12:00:00Z"

      // 1. Producer: votes-pipeline-style write — same call site BillLookup.upsertPlaceholder routes through.
      val _ = billRepo.upsertPlaceholder(naturalKey).transact(xa).unsafeRunSync()

      // 2. Pre-state assertion — confirm the row matches the canonical placeholder shape that
      // BillPlaceholder.isPlaceholder is pinned to (post-fix: update_date is NULL, every detail Option is None).
      val pre = billRepo.findByBillId(naturalKey).transact(xa).unsafeRunSync()
      val _   = pre.isDefined shouldBe true
      val _ = pre.foreach { b =>
        val _ = BillPlaceholder.isPlaceholder(b) shouldBe true
        val _ = b.title shouldBe ""
        b.updateDate shouldBe None
      }

      // 3. Run the metadata processor's per-item flow — exactly what `streamAll` does for each list item.
      val processor     = buildProcessor(stubApi(naturalKey, realTitle, realUpdate))
      val correlationId = UUID.randomUUID()
      val result        = processor.processListItem(makeListItem(naturalKey), correlationId).unsafeRunSync()

      // 4. Post-state assertions.
      val _ = result.isSucceeded shouldBe true

      val post = billRepo.findByBillId(naturalKey).transact(xa).unsafeRunSync()
      val _    = post.isDefined shouldBe true
      val _ = post.foreach { b =>
        val _ = b.title shouldBe realTitle
        val _ = b.updateDate.isDefined shouldBe true
        // Critical: post-fix the row no longer matches the placeholder shape.
        BillPlaceholder.isPlaceholder(b) shouldBe false
      }

      // 5. The bill_history archive must contain the pre-enrichment placeholder snapshot. This is the assertion
      // that proves migration 034 (bill_history.update_date nullable) is load-bearing — pre-migration this
      // INSERT failed with "null value in column update_date violates not-null constraint" and the entire
      // round-trip was blocked at the archive step.
      val historyCount = sql"""SELECT COUNT(*) FROM bill_history bh
                              JOIN bills b ON b.id = bh.bill_id
                              WHERE b.congress = 119 AND b.number = 1::int AND bh.title = ''"""
        .query[Int]
        .unique
        .transact(xa)
        .unsafeRunSync()
      val _ = historyCount shouldBe 1

      // Sanity: still exactly one bill row at this natural key (upsert did not duplicate).
      countBills(naturalKey) shouldBe 1
    }

  // ------------------------------------------------------------------------
  // Test 2: Summary-pipeline placeholder + advanced expected_version → metadata enriches
  //         WITHOUT regressing the expected stage
  // ------------------------------------------------------------------------

  "Round-trip: summary-pipeline placeholder with advanced expected_version → metadata enriches" should
    "preserve the summary-set ENR despite metadata's IH/IS floor write attempt" taggedAs DockerRequired in {
      val naturalKey = "119-HR-2"
      val realTitle  = "An Act with a summary-advanced expected_text_version_code"
      val realUpdate = "2025-09-01T00:00:00Z"

      // 1. Producer step A: summary-pipeline-style placeholder write — same call site
      // BillSummaryProcessor.processOneBill routes through.
      val _ = billRepo.upsertPlaceholder(naturalKey).transact(xa).unsafeRunSync()

      // 2. Producer step B: summary-pipeline advances expected_text_version_code from CRS summary's versionCode.
      // ENR is high in TextVersionCode.progressionOrder — way past the IH floor that metadata-pipeline would
      // try to write on its enrichment pass.
      val _ = billRepo.updateExpectedVersion(naturalKey, TextVersionCode.ENR).transact(xa).unsafeRunSync()
      val _ = billRepo.findExpectedVersion(naturalKey).transact(xa).unsafeRunSync() shouldBe Some(
        TextVersionCode.ENR
      )

      // 3. Run the metadata processor — its BillPersister.applyExpectedVersionFloor will read the current
      // expected_text_version_code (ENR), compute the chamber floor (IH for House), compare progressionOrders,
      // and SKIP the write because IH < ENR. That's the cross-pipeline regression guard at work.
      val processor     = buildProcessor(stubApi(naturalKey, realTitle, realUpdate))
      val correlationId = UUID.randomUUID()
      val result        = processor.processListItem(makeListItem(naturalKey), correlationId).unsafeRunSync()

      val _ = result.isSucceeded shouldBe true

      // 4. Post-state: the bill is enriched but expected_text_version_code is still ENR — the summary's signal
      // wasn't clobbered by the metadata floor. If this regresses to IH, bill-text-availability-checker would
      // start polling the wrong version code and the whole stage-aware filter from migration 032 falls apart.
      val post = billRepo.findByBillId(naturalKey).transact(xa).unsafeRunSync()
      val _    = post.isDefined shouldBe true
      val _ = post.foreach { b =>
        val _ = b.title shouldBe realTitle
        BillPlaceholder.isPlaceholder(b) shouldBe false
      }

      val storedExpected = billRepo.findExpectedVersion(naturalKey).transact(xa).unsafeRunSync()
      storedExpected shouldBe Some(TextVersionCode.ENR)
    }

}
