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
  // Happy path: bill-linked senate vote produces a populated billNaturalKey + legislationType/Number
  // ------------------------------------------------------------------

  "convert" should "produce a Senate VoteConversionResult with billNaturalKey populated for a bill-like document" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
    val dto = senateDto(
      document = billDoc(docType = "S.", docNumber = "1071", docCongress = 119, docTitle = "Title"),
      members = List(senator("S428", voteCast = "Yea"), senator("S429", voteCast = "Nay")),
    )

    val result = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = result.vote.naturalKey shouldBe "119-Senate-1-17"
    val _ = result.vote.chamber shouldBe Chamber.Senate
    // billId is NOT set by the converter — the processor resolves billNaturalKey to an id.
    val _ = result.vote.billId shouldBe None
    // billNaturalKey derived from the document.
    val _ = result.billNaturalKey shouldBe Some("119-S-1071")
    val _ = result.vote.legislationType shouldBe Some(BillType.S)
    val _ = result.vote.legislationNumber shouldBe Some("1071")
    val _ = result.vote.voteType shouldBe Some(VoteType.Passage)
    val _ = result.vote.voteDate shouldBe Some(LocalDate.parse("2025-01-25"))
    // Positions are unresolved with Right(lisId)
    val _ = result.positions.length shouldBe 2
    result.positions.map(_.memberSource).toSet shouldBe Set(Left(""), Right("S428"), Right("S429")) - Left("")
  }

  it should "classify H.R. documents correctly" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
    val dto       = senateDto(document = billDoc(docType = "H.R.", docNumber = "1319", docCongress = 117))
    val result    = converter.convert(dto, logCtx).unsafeRunSync()
    val _         = result.billNaturalKey shouldBe Some("117-HR-1319")
    result.vote.legislationType shouldBe Some(BillType.HR)
  }

  it should "classify all resolution types" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
    val cases = List(
      ("S.J.Res.", BillType.SJRES),
      ("H.J.Res.", BillType.HJRES),
      ("S.Res.", BillType.SRES),
      ("H.Res.", BillType.HRES),
      ("S.Con.Res.", BillType.SCONRES),
      ("H.Con.Res.", BillType.HCONRES),
    )
    cases.foreach {
      case (docType, expected) =>
        val dto    = senateDto(document = billDoc(docType = docType, docNumber = "5"))
        val result = converter.convert(dto, logCtx).unsafeRunSync()
        val _      = result.vote.legislationType shouldBe Some(expected)
        result.billNaturalKey shouldBe Some(s"119-${expected.apiValue.toUpperCase}-5")
    }
  }

  // ------------------------------------------------------------------
  // Non-bill documents: PN, Treaty, unknown — billNaturalKey = None, legislationType = None
  // ------------------------------------------------------------------

  it should "produce billNaturalKey = None for Presidential Nomination documents" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
    val dto       = senateDto(document = nominationDoc)

    val result = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = result.billNaturalKey shouldBe None
    val _ = result.vote.legislationType shouldBe None
    result.vote.legislationNumber shouldBe None
  }

  it should "produce billNaturalKey = None for Treaty documents" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
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
    val result = converter.convert(dto, logCtx).unsafeRunSync()
    result.billNaturalKey shouldBe None
  }

  it should "produce billNaturalKey = None and warn for unknown documentType" in {
    val loggerMock = mkLogger
    val converter  = new SenateVoteConverter[IO](loggerMock)
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

    val result = converter.convert(dto, logCtx).unsafeRunSync()

    val _ = result.billNaturalKey shouldBe None
    import org.mockito.Mockito.{atLeastOnce, verify}
    verify(loggerMock, atLeastOnce()).warn(any[LogContext], anyString())
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
  // Position materialization — always UnresolvedVotePosition with Right(lisMemberId)
  // ------------------------------------------------------------------

  "buildUnresolvedPositions" should "emit Right(lisMemberId) for every senator" in {
    val converter = new SenateVoteConverter[IO](mkLogger)
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
