package repcheck.ingestion.votes.pipeline

import java.time.LocalDate

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dto.vote.{SenateVoteDocumentDTO, SenateVoteMemberXmlDTO, SenateVoteXmlDTO}
import repcheck.shared.models.congress.vote.VoteType

class SenateVoteConverterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val logCtx = LogContext(runId = "r", stepName = "test")

  /** Test base URL — the converter just passes this through to `SenateVoteUrls`. */
  private val testBaseUrl = "https://www.senate.gov/legislative/LIS"

  /** Stub that returns `Some(id)` for every key. */
  private def stubBillLookup(resolved: Long): String => IO[Option[Long]] =
    _ => IO.pure(Some(resolved))

  /** Recording stub to verify `billLookup` call count / arguments. */
  private def recordingBillLookup(
    target: java.util.concurrent.atomic.AtomicReference[List[String]]
  ): String => IO[Option[Long]] =
    nk => IO.delay(target.updateAndGet(_ :+ nk)).as(Some(999L))

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  private def senator(
    lisId: String,
    firstName: String = "Angela",
    lastName: String = "Alsobrooks",
    party: String = "D",
    state: String = "MD",
    voteCast: String = "Yea",
  ): SenateVoteMemberXmlDTO =
    SenateVoteMemberXmlDTO(
      lisMemberId = lisId,
      firstName = firstName,
      lastName = lastName,
      party = party,
      state = state,
      voteCast = voteCast,
    )

  /** A bill-like document the converter should classify with a bill natural key. */
  private def billDoc(
    docType: String = "S.",
    docNumber: String = "1071",
    docCongress: Int = 119,
    docTitle: String = "A bill title",
  ): SenateVoteDocumentDTO =
    SenateVoteDocumentDTO(
      documentCongress = docCongress,
      documentType = docType,
      documentNumber = docNumber,
      documentName = s"$docType $docNumber",
      documentTitle = docTitle,
      documentShortTitle = None,
    )

  /** A Presidential-Nomination document the converter should classify as billId-unlinked. */
  private def nominationDoc: SenateVoteDocumentDTO =
    SenateVoteDocumentDTO(
      documentCongress = 119,
      documentType = "PN",
      documentNumber = "11-11",
      documentName = "PN11-11",
      documentTitle = "Kristi Noem, of South Dakota, to be Secretary of Homeland Security",
      documentShortTitle = None,
    )

  private def senateDto(
    congress: Int = 119,
    session: Int = 1,
    voteNumber: Int = 17,
    question: String = "On Passage of the Bill",
    voteDate: String = "January 25, 2025, 11:30 AM",
    result: String = "Bill Passed",
    document: SenateVoteDocumentDTO = billDoc(),
    members: List[SenateVoteMemberXmlDTO] = List.empty,
  ): SenateVoteXmlDTO =
    SenateVoteXmlDTO(
      congress = congress,
      session = session,
      voteNumber = voteNumber,
      question = question,
      voteDate = voteDate,
      result = result,
      document = document,
      members = members,
    )

  // ------------------------------------------------------------------
  // Happy path: bill-linked senate vote → billLookup invoked, billId populated, URLs derived
  // ------------------------------------------------------------------

  "convert" should "populate billId via billLookup, derive legislationUrl + sourceDataUrl for a bill-like document" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto = senateDto(
      document = billDoc(docType = "S.", docNumber = "1071", docCongress = 119, docTitle = "Title"),
      members = List(senator("S428", voteCast = "Yea"), senator("S429", voteCast = "Nay")),
    )

    val result = converter.convert(dto, stubBillLookup(500L), logCtx).unsafeRunSync()

    val _ = result.vote.naturalKey shouldBe "119-Senate-1-17"
    val _ = result.vote.chamber shouldBe Chamber.Senate
    // billId resolved via billLookup — NOT None.
    val _ = result.vote.billId shouldBe Some(500L)
    // billNaturalKey derived from the document for event-emission bookkeeping.
    val _ = result.billNaturalKey shouldBe Some("119-S-1071")
    val _ = result.vote.legislationType shouldBe Some(BillType.S)
    val _ = result.vote.legislationNumber shouldBe Some("1071")
    // legislationUrl derived from document (BillType.S → senate-bill).
    val _ = result.vote.legislationUrl shouldBe Some("https://www.congress.gov/bill/119/senate-bill/1071")
    // sourceDataUrl derived via SenateVoteUrls.voteXmlUrl — matches the URL pattern senate.gov actually serves.
    val _ = result.vote.sourceDataUrl shouldBe Some(
      "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00017.xml"
    )
    val _ = result.vote.voteType shouldBe Some(VoteType.Passage)
    val _ = result.vote.voteDate shouldBe Some(LocalDate.parse("2025-01-25"))
    // Positions are unresolved with Right(lisId)
    val _ = result.positions.length shouldBe 2
    result.positions.map(_.memberSource).toSet shouldBe Set(Right("S428"), Right("S429"))
  }

  it should "call billLookup exactly once per converted vote (not per senator)" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto       = senateDto(members = List(senator("S1"), senator("S2"), senator("S3")))

    val calls = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val _     = converter.convert(dto, recordingBillLookup(calls), logCtx).unsafeRunSync()

    calls.get() shouldBe List("119-S-1071")
  }

  it should "classify H.R. documents and resolve billId via billLookup" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto       = senateDto(document = billDoc(docType = "H.R.", docNumber = "1319", docCongress = 117))

    val result = converter.convert(dto, stubBillLookup(321L), logCtx).unsafeRunSync()

    val _ = result.billNaturalKey shouldBe Some("117-HR-1319")
    val _ = result.vote.legislationType shouldBe Some(BillType.HR)
    val _ = result.vote.billId shouldBe Some(321L)
    result.vote.legislationUrl shouldBe Some("https://www.congress.gov/bill/117/house-bill/1319")
  }

  it should "classify all resolution types and derive the correct URL slug for each" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val cases = List(
      ("S.J.Res.", BillType.SJRES, "senate-joint-resolution"),
      ("H.J.Res.", BillType.HJRES, "house-joint-resolution"),
      ("S.Res.", BillType.SRES, "senate-resolution"),
      ("H.Res.", BillType.HRES, "house-resolution"),
      ("S.Con.Res.", BillType.SCONRES, "senate-concurrent-resolution"),
      ("H.Con.Res.", BillType.HCONRES, "house-concurrent-resolution"),
    )
    cases.foreach {
      case (docType, expectedBillType, expectedSlug) =>
        val dto    = senateDto(document = billDoc(docType = docType, docNumber = "5"))
        val result = converter.convert(dto, stubBillLookup(42L), logCtx).unsafeRunSync()
        val _      = result.vote.legislationType shouldBe Some(expectedBillType)
        val _      = result.vote.billId shouldBe Some(42L)
        val _      = result.billNaturalKey shouldBe Some(s"119-${expectedBillType.apiValue.toUpperCase}-5")
        result.vote.legislationUrl shouldBe Some(s"https://www.congress.gov/bill/119/$expectedSlug/5")
    }
  }

  // ------------------------------------------------------------------
  // Non-bill documents: PN, Treaty, unknown — billLookup NOT invoked, billId = None, URLs not derived
  // ------------------------------------------------------------------

  it should "produce billId = None and skip billLookup for Presidential Nomination documents" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto       = senateDto(document = nominationDoc)

    val calls = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val result = converter
      .convert(dto, recordingBillLookup(calls), logCtx)
      .unsafeRunSync()

    val _ = result.billNaturalKey shouldBe None
    val _ = result.vote.billId shouldBe None
    val _ = result.vote.legislationType shouldBe None
    val _ = result.vote.legislationNumber shouldBe None
    val _ = result.vote.legislationUrl shouldBe None
    // sourceDataUrl is still populated — every senate vote has a known URL regardless of document type.
    val _ = result.vote.sourceDataUrl.isDefined shouldBe true
    // billLookup was NEVER invoked.
    calls.get() shouldBe List.empty
  }

  it should "produce billId = None and skip billLookup for Treaty documents" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto = senateDto(
      document = SenateVoteDocumentDTO(
        documentCongress = 119,
        documentType = "Treaty Doc.",
        documentNumber = "116-4",
        documentName = "Treaty Doc. 116-4",
        documentTitle = "Some treaty",
        documentShortTitle = None,
      )
    )

    val calls  = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val result = converter.convert(dto, recordingBillLookup(calls), logCtx).unsafeRunSync()

    val _ = result.billNaturalKey shouldBe None
    val _ = result.vote.billId shouldBe None
    val _ = result.vote.legislationUrl shouldBe None
    calls.get() shouldBe List.empty
  }

  it should "produce billId = None, skip billLookup, and warn for unknown documentType" in {
    val loggerMock = mkLogger
    val converter  = new SenateVoteConverter[IO](loggerMock, testBaseUrl)
    val dto = senateDto(
      document = SenateVoteDocumentDTO(
        documentCongress = 119,
        documentType = "Future.New.Type",
        documentNumber = "1",
        documentName = "Future.New.Type 1",
        documentTitle = "Something new",
        documentShortTitle = None,
      )
    )

    val calls  = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val result = converter.convert(dto, recordingBillLookup(calls), logCtx).unsafeRunSync()

    val _ = result.billNaturalKey shouldBe None
    val _ = result.vote.billId shouldBe None
    val _ = calls.get() shouldBe List.empty
    import org.mockito.Mockito.{atLeastOnce, verify}
    verify(loggerMock, atLeastOnce()).warn(any[LogContext], anyString())
  }

  it should "produce billId = None and skip billLookup when documentType is a bill but documentCongress is 0" in {
    // Older Senate-vote XML (109th Congress era and earlier) sometimes emits a fully-populated <document> block —
    // documentType, documentNumber, documentName all real — but a self-closing or empty <document_congress/>.
    // SenateVoteXmlDecoder.decodeDocument tolerates this by defaulting documentCongress to 0 (rather than dropping
    // the whole vote). Without the > 0 gate in classifyDocument, the converter would build a bill natural key like
    // "0-S-1059" and BillRepository.upsertPlaceholder would create an orphan congress=0 placeholder bill that no
    // future write can heal (the (congress, bill_type, number) UNIQUE constraint means a later real (118, S, 1059)
    // upsert lands as a separate row). 909 such orphans surfaced empirically before this gate.
    val loggerMock = mkLogger
    val converter  = new SenateVoteConverter[IO](loggerMock, testBaseUrl)
    val dto = senateDto(
      document = billDoc(docType = "S.", docNumber = "1059", docCongress = 0, docTitle = "Old vote, missing congress")
    )

    val calls  = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val result = converter.convert(dto, recordingBillLookup(calls), logCtx).unsafeRunSync()

    val _ = result.billNaturalKey shouldBe None
    val _ = result.vote.billId shouldBe None
    val _ = result.vote.legislationType shouldBe None
    val _ = result.vote.legislationNumber shouldBe None
    val _ = result.vote.legislationUrl shouldBe None
    // billLookup must NOT have been invoked — the whole point is to avoid creating the orphan placeholder.
    val _ = calls.get() shouldBe List.empty
    // Logged at INFO (expected-for-old-votes case), NOT warn.
    import org.mockito.Mockito.{atLeastOnce, verify}
    verify(loggerMock, atLeastOnce()).info(any[LogContext], anyString())
  }

  it should "treat negative documentCongress identically to 0 (defensive lower-bound check)" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto = senateDto(
      document = billDoc(docType = "HR.", docNumber = "42", docCongress = -1)
    )

    val calls  = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)
    val result = converter.convert(dto, recordingBillLookup(calls), logCtx).unsafeRunSync()

    val _ = result.billNaturalKey shouldBe None
    val _ = result.vote.billId shouldBe None
    calls.get() shouldBe List.empty
  }

  // ------------------------------------------------------------------
  // billLookup error propagation
  // ------------------------------------------------------------------

  it should "propagate errors raised by billLookup without wrapping" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto       = senateDto(document = billDoc())
    val failing: String => IO[Option[Long]] =
      _ => IO.raiseError(new IllegalStateException("simulated bill resolution failure"))

    val outcome = converter.convert(dto, failing, logCtx).attempt.unsafeRunSync()

    outcome match {
      case Left(e: IllegalStateException) =>
        e.getMessage should include("simulated bill resolution failure")
      case other => fail(s"expected Left(IllegalStateException), got $other")
    }
  }

  // ------------------------------------------------------------------
  // sourceDataUrl is always populated — even for non-bill votes
  // ------------------------------------------------------------------

  "sourceDataUrl" should "be derived for every vote regardless of document type" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dtoPn     = senateDto(voteNumber = 17, document = nominationDoc)
    val dtoBill   = senateDto(voteNumber = 648, document = billDoc())

    val resultPn   = converter.convert(dtoPn, stubBillLookup(1L), logCtx).unsafeRunSync()
    val resultBill = converter.convert(dtoBill, stubBillLookup(1L), logCtx).unsafeRunSync()

    val _ = resultPn.vote.sourceDataUrl shouldBe Some(
      "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00017.xml"
    )
    resultBill.vote.sourceDataUrl shouldBe Some(
      "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1191/vote_119_1_00648.xml"
    )
  }

  it should "respect the configured senateBaseUrl when deriving sourceDataUrl" in {
    val converter = new SenateVoteConverter[IO](mkLogger, "http://127.0.0.1:9999/mocked")
    val dto       = senateDto(voteNumber = 17, document = nominationDoc)

    val result = converter.convert(dto, stubBillLookup(1L), logCtx).unsafeRunSync()

    result.vote.sourceDataUrl shouldBe Some(
      "http://127.0.0.1:9999/mocked/roll_call_votes/vote1191/vote_119_1_00017.xml"
    )
  }

  // ------------------------------------------------------------------
  // normalizeDocumentType — pure function
  // ------------------------------------------------------------------

  "normalizeDocumentType" should "return Right(BillType) for every bill-like input" in {
    import SenateVoteConverter.normalizeDocumentType
    val _ = normalizeDocumentType("S.") shouldBe Right(BillType.S)
    val _ = normalizeDocumentType("H.R.") shouldBe Right(BillType.HR)
    val _ = normalizeDocumentType("S.J.Res.") shouldBe Right(BillType.SJRES)
    val _ = normalizeDocumentType("H.J.Res.") shouldBe Right(BillType.HJRES)
    val _ = normalizeDocumentType("S.Res.") shouldBe Right(BillType.SRES)
    val _ = normalizeDocumentType("H.Res.") shouldBe Right(BillType.HRES)
    val _ = normalizeDocumentType("S.Con.Res.") shouldBe Right(BillType.SCONRES)
    normalizeDocumentType("H.Con.Res.") shouldBe Right(BillType.HCONRES)
  }

  it should "return Left(NonBillDocument) for PN and Treaty Doc." in {
    import SenateVoteConverter.{NonBillDocument, normalizeDocumentType}
    val _ = normalizeDocumentType("PN") shouldBe Left(NonBillDocument("PN"))
    normalizeDocumentType("Treaty Doc.") shouldBe Left(NonBillDocument("Treaty Doc."))
  }

  it should "return Left(UnknownDocument) for anything else" in {
    import SenateVoteConverter.{UnknownDocument, normalizeDocumentType}
    normalizeDocumentType("Bogus") shouldBe Left(UnknownDocument("Bogus"))
  }

  // ------------------------------------------------------------------
  // buildCongressGovBillUrl / billTypeUrlSlug — unit coverage including non-bill BillType variants
  // ------------------------------------------------------------------

  "buildCongressGovBillUrl" should "produce the expected congress.gov URL for every bill-like BillType" in {
    import SenateVoteConverter.buildCongressGovBillUrl
    val _ = buildCongressGovBillUrl(119, BillType.S, "1071") shouldBe
      Some("https://www.congress.gov/bill/119/senate-bill/1071")
    val _ = buildCongressGovBillUrl(119, BillType.HR, "30") shouldBe
      Some("https://www.congress.gov/bill/119/house-bill/30")
    val _ = buildCongressGovBillUrl(118, BillType.SJRES, "5") shouldBe
      Some("https://www.congress.gov/bill/118/senate-joint-resolution/5")
    val _ = buildCongressGovBillUrl(118, BillType.HJRES, "1") shouldBe
      Some("https://www.congress.gov/bill/118/house-joint-resolution/1")
    val _ = buildCongressGovBillUrl(119, BillType.SRES, "8") shouldBe
      Some("https://www.congress.gov/bill/119/senate-resolution/8")
    val _ = buildCongressGovBillUrl(119, BillType.HRES, "5") shouldBe
      Some("https://www.congress.gov/bill/119/house-resolution/5")
    val _ = buildCongressGovBillUrl(119, BillType.SCONRES, "1") shouldBe
      Some("https://www.congress.gov/bill/119/senate-concurrent-resolution/1")
    buildCongressGovBillUrl(119, BillType.HCONRES, "14") shouldBe
      Some("https://www.congress.gov/bill/119/house-concurrent-resolution/14")
  }

  it should "return None for non-bill BillType variants (PL, STAT, USC, SRPT, HRPT)" in {
    import SenateVoteConverter.buildCongressGovBillUrl
    val _ = buildCongressGovBillUrl(119, BillType.PL, "100") shouldBe None
    val _ = buildCongressGovBillUrl(119, BillType.STAT, "1") shouldBe None
    val _ = buildCongressGovBillUrl(119, BillType.USC, "1") shouldBe None
    val _ = buildCongressGovBillUrl(119, BillType.SRPT, "1") shouldBe None
    buildCongressGovBillUrl(119, BillType.HRPT, "1") shouldBe None
  }

  // ------------------------------------------------------------------
  // Position materialization — always UnresolvedVotePosition with Right(lisMemberId)
  // ------------------------------------------------------------------

  "buildUnresolvedPositions" should "emit Right(lisMemberId) for every senator" in {
    val converter = new SenateVoteConverter[IO](mkLogger, testBaseUrl)
    val dto       = senateDto(members = List(senator("S428"), senator("S429")))

    val positions = converter.buildUnresolvedPositions(dto)

    val _ = positions.length shouldBe 2
    positions.flatMap(_.memberSource.toOption) shouldBe List("S428", "S429")
  }

  // ------------------------------------------------------------------
  // parseVoteDate format variants (unchanged from the previous spec)
  // ------------------------------------------------------------------

  "parseVoteDate" should "parse 'January 25, 2025, 11:30 AM' (long form no day-of-week)" in {
    SenateVoteConverter.parseVoteDate("January 25, 2025, 11:30 AM") shouldBe Some(
      LocalDate.parse("2025-01-25")
    )
  }

  it should "parse 'Thursday, April 3, 2025, 02:42 PM' (long form with day-of-week)" in {
    SenateVoteConverter.parseVoteDate("Thursday, April 3, 2025, 02:42 PM") shouldBe Some(
      LocalDate.parse("2025-04-03")
    )
  }

  it should "parse ISO-offset '2025-01-25T11:30:00Z' date-times" in {
    SenateVoteConverter.parseVoteDate("2025-01-25T11:30:00Z") shouldBe Some(
      LocalDate.parse("2025-01-25")
    )
  }

  it should "tolerate extra interior whitespace" in {
    SenateVoteConverter.parseVoteDate("January  25,  2025,  11:30 AM") shouldBe Some(
      LocalDate.parse("2025-01-25")
    )
  }

  it should "return None for unparseable inputs (decoder already filters these upstream)" in {
    SenateVoteConverter.parseVoteDate("not a date") shouldBe None
  }

}
