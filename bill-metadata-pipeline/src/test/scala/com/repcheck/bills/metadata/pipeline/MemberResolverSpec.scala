package com.repcheck.bills.metadata.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{EntityRepository, PlaceholderCreator}
import repcheck.shared.models.congress.dos.bill.BillDO
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.bill.{BillDetailDTO, CoSponsorDTO, SponsorDTO}
import repcheck.shared.models.placeholder.HasPlaceholder

import com.repcheck.bills.common.persistence.MemberLookupRepository

class MemberResolverSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:memberresolver;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val logCtx = LogContext("test-run", "test-step")

  private class StubPlaceholderCreator extends PlaceholderCreator[IO] {
    private val callsRef = new java.util.concurrent.atomic.AtomicReference[List[String]](List.empty)

    def ensureExists[T <: Product](
      naturalKey: String,
      repository: EntityRepository[IO, T],
    )(using HasPlaceholder[T]): IO[Unit] = IO {
      val _ = callsRef.updateAndGet(keys => keys :+ naturalKey)
    }

    def callCount: Int = callsRef.get().size
  }

  private def makeResolver(
    memberLookup: MemberLookupRepository[ConnectionIO],
    loggerMock: PipelineLogger[IO],
  ): MemberResolver[IO] = {
    val stub          = new StubPlaceholderCreator
    val memberEntRepo = mock[EntityRepository[IO, MemberDO]]
    new MemberResolver[IO](memberLookup, stub, memberEntRepo, testXa, loggerMock)
  }

  private def makeLogger(): PipelineLogger[IO] = {
    val l = mock[PipelineLogger[IO]]
    when(l.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(l.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(l.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    when(l.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    l
  }

  private val baseBillDO = BillDO(
    billId = 1L,
    naturalKey = "118-HR-1",
    congress = 118,
    billType = "hr",
    number = "1",
    title = "Test",
    originChamber = None,
    originChamberCode = None,
    introducedDate = None,
    policyArea = None,
    latestActionDate = None,
    latestActionText = None,
    constitutionalAuthorityText = None,
    sponsorMemberId = None,
    textUrl = None,
    textFormat = None,
    textVersionType = None,
    textDate = None,
    textContent = None,
    textEmbedding = None,
    summaryText = None,
    summaryActionDesc = None,
    summaryActionDate = None,
    updateDate = None,
    updateDateIncludingText = None,
    legislationUrl = None,
    apiUrl = None,
    createdAt = None,
    updatedAt = None,
    latestTextVersionId = None,
  )

  private def makeDetail(sponsors: Option[List[SponsorDTO]]): BillDetailDTO =
    BillDetailDTO(
      congress = 118,
      number = "1",
      billType = "hr",
      latestAction = None,
      originChamber = None,
      originChamberCode = None,
      title = "Test",
      updateDate = None,
      updateDateIncludingText = None,
      url = "https://example.com",
      introducedDate = None,
      policyArea = None,
      sponsors = sponsors,
      cosponsors = None,
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

  "ensureSponsorPlaceholder" should "set sponsorMemberId when member is found" in {
    val lookup = mock[MemberLookupRepository[ConnectionIO]]
    when(lookup.findIdByNaturalKey("S001")).thenReturn(doobie.free.connection.pure(Some(42L)))
    val logger   = makeLogger()
    val resolver = makeResolver(lookup, logger)
    val sponsor  = SponsorDTO("S001", None, None, None, None, None, None, None, None)
    val detail   = makeDetail(sponsors = Some(List(sponsor)))

    val result = resolver.ensureSponsorPlaceholder(baseBillDO, detail, logCtx).unsafeRunSync()
    result.sponsorMemberId shouldBe Some(42L)
  }

  it should "set sponsorMemberId to None and warn when member is not found" in {
    val lookup = mock[MemberLookupRepository[ConnectionIO]]
    when(lookup.findIdByNaturalKey("GHOST")).thenReturn(doobie.free.connection.pure(Option.empty[Long]))
    val logger   = makeLogger()
    val resolver = makeResolver(lookup, logger)
    val sponsor  = SponsorDTO("GHOST", None, None, None, None, None, None, None, None)
    val detail   = makeDetail(sponsors = Some(List(sponsor)))

    val result = resolver.ensureSponsorPlaceholder(baseBillDO, detail, logCtx).unsafeRunSync()
    val _      = result.sponsorMemberId shouldBe None
    verify(logger, times(1)).warn(any[LogContext], org.mockito.ArgumentMatchers.contains("GHOST"))
  }

  it should "return bill unchanged when no sponsors exist" in {
    val lookup   = mock[MemberLookupRepository[ConnectionIO]]
    val logger   = makeLogger()
    val resolver = makeResolver(lookup, logger)
    val detail   = makeDetail(sponsors = None)

    val result = resolver.ensureSponsorPlaceholder(baseBillDO, detail, logCtx).unsafeRunSync()
    val _      = result.sponsorMemberId shouldBe None
    verify(lookup, never()).findIdByNaturalKey(anyString())
  }

  "buildCosponsorDOs" should "resolve all cosponsors when all members are found" in {
    val lookup = mock[MemberLookupRepository[ConnectionIO]]
    when(lookup.findIdByNaturalKey("C001")).thenReturn(doobie.free.connection.pure(Some(10L)))
    when(lookup.findIdByNaturalKey("C002")).thenReturn(doobie.free.connection.pure(Some(20L)))
    val logger   = makeLogger()
    val resolver = makeResolver(lookup, logger)
    val dtos = List(
      CoSponsorDTO("C001", None, None, None, Some(true), None, None, Some("2024-01-01"), None, None),
      CoSponsorDTO("C002", None, None, None, Some(false), None, None, None, None, None),
    )

    val result = resolver.buildCosponsorDOs(dtos, logCtx).unsafeRunSync()
    val _      = result.size shouldBe 2
    val _      = result.headOption.map(_.memberId) shouldBe Some(10L)
    result.lift(1).map(_.memberId) shouldBe Some(20L)
  }

  it should "filter out cosponsors when member lookup returns None" in {
    val lookup = mock[MemberLookupRepository[ConnectionIO]]
    when(lookup.findIdByNaturalKey("FOUND")).thenReturn(doobie.free.connection.pure(Some(10L)))
    when(lookup.findIdByNaturalKey("MISSING")).thenReturn(doobie.free.connection.pure(Option.empty[Long]))
    val logger   = makeLogger()
    val resolver = makeResolver(lookup, logger)
    val dtos = List(
      CoSponsorDTO("FOUND", None, None, None, Some(true), None, None, None, None, None),
      CoSponsorDTO("MISSING", None, None, None, Some(false), None, None, None, None, None),
    )

    val result = resolver.buildCosponsorDOs(dtos, logCtx).unsafeRunSync()
    val _      = result.size shouldBe 1
    val _      = result.headOption.map(_.memberId) shouldBe Some(10L)
    verify(logger, times(1)).warn(any[LogContext], org.mockito.ArgumentMatchers.contains("MISSING"))
  }

  it should "return empty list when all cosponsors fail lookup" in {
    val lookup = mock[MemberLookupRepository[ConnectionIO]]
    when(lookup.findIdByNaturalKey(anyString())).thenReturn(doobie.free.connection.pure(Option.empty[Long]))
    val logger   = makeLogger()
    val resolver = makeResolver(lookup, logger)
    val dtos = List(
      CoSponsorDTO("GONE1", None, None, None, None, None, None, None, None, None),
      CoSponsorDTO("GONE2", None, None, None, None, None, None, None, None, None),
    )

    val result = resolver.buildCosponsorDOs(dtos, logCtx).unsafeRunSync()
    result shouldBe empty
  }

}
