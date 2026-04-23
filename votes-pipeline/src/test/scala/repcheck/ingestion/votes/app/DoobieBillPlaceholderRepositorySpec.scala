package repcheck.ingestion.votes.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.votes.errors.BillResolutionFailed
import repcheck.shared.models.congress.common.BillType
import repcheck.shared.models.congress.dos.bill.BillDO

/**
 * Unit spec for [[DoobieBillPlaceholderRepository]]'s natural-key parser. The parser is the interesting piece — the
 * actual INSERT statement is exercised by the integration spec in the same file (via `DockerRequired`-tagged tests in
 * [[DoobieBillPlaceholderRepositoryIntegrationSpec]] below).
 *
 * Parser contract (see class docstring): accept only `"<congress>-<BILL_TYPE>-<number>"` strings as emitted by
 * `BillConversions.buildBillNaturalKey`. Anything else raises [[BillResolutionFailed]] carrying the offending key so
 * operators can trace the bad input through the per-vote failure isolation path.
 */
class DoobieBillPlaceholderRepositorySpec extends AnyFlatSpec with Matchers {

  // The parser is pure and doesn't touch the transactor, so a stub transactor with an H2-style URL is fine. The tests
  // never `insertIfNotExists` here — they call `parseNaturalKey` directly.
  private val stubXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:bill-placeholder-parser;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val repo = new DoobieBillPlaceholderRepository[IO](stubXa)

  "parseNaturalKey" should "accept the canonical '<congress>-<TYPE>-<number>' form" in {
    repo.parseNaturalKey("119-HR-30") shouldBe Right((119, BillType.HR, 30))
  }

  it should "accept any known BillType (uppercase)" in {
    val _ = repo.parseNaturalKey("118-S-42") shouldBe Right((118, BillType.S, 42))
    val _ = repo.parseNaturalKey("119-HJRES-1") shouldBe Right((119, BillType.HJRES, 1))
    val _ = repo.parseNaturalKey("119-SJRES-5") shouldBe Right((119, BillType.SJRES, 5))
    val _ = repo.parseNaturalKey("119-HRES-100") shouldBe Right((119, BillType.HRES, 100))
    repo.parseNaturalKey("119-SRES-200") shouldBe Right((119, BillType.SRES, 200))
  }

  it should "accept BillType in any case (fromString uppercases)" in {
    val _ = repo.parseNaturalKey("119-hr-30") shouldBe Right((119, BillType.HR, 30))
    repo.parseNaturalKey("119-Hr-30") shouldBe Right((119, BillType.HR, 30))
  }

  it should "return Left(BillResolutionFailed) for a key with fewer than 3 segments" in {
    val outcome = repo.parseNaturalKey("119-HR")
    outcome match {
      case Left(e: BillResolutionFailed) =>
        val _ = e.billNaturalKey shouldBe "119-HR"
        e.getMessage should include("3 '-' segments")
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

  it should "return Left(BillResolutionFailed) for a key with no separator at all" in {
    val outcome = repo.parseNaturalKey("no-separator")
    outcome match {
      case Left(e: BillResolutionFailed) =>
        val _ = e.billNaturalKey shouldBe "no-separator"
        e.getMessage should include("3 '-' segments")
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

  it should "return Left(BillResolutionFailed) when congress is not an int" in {
    val outcome = repo.parseNaturalKey("abc-HR-30")
    outcome match {
      case Left(e: BillResolutionFailed) =>
        val _ = e.billNaturalKey shouldBe "abc-HR-30"
        e.getMessage should include("congress segment 'abc' is not an int")
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

  it should "return Left(BillResolutionFailed) when bill_type is unrecognized" in {
    val outcome = repo.parseNaturalKey("119-BOGUS-30")
    outcome match {
      case Left(e: BillResolutionFailed) =>
        val _ = e.billNaturalKey shouldBe "119-BOGUS-30"
        e.getMessage should include("Unrecognized BillType")
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

  it should "return Left(BillResolutionFailed) when number is not an int" in {
    val outcome = repo.parseNaturalKey("119-HR-thirty")
    outcome match {
      case Left(e: BillResolutionFailed) =>
        val _ = e.billNaturalKey shouldBe "119-HR-thirty"
        e.getMessage should include("number segment 'thirty' is not an int")
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

  "insertIfNotExists" should "raise BillResolutionFailed up front when the natural key is malformed, without touching the transactor" in {
    // The stubbed transactor points at an H2 database that never had a `bills` table — if the repository tried to run
    // the INSERT, it would fail with a schema error rather than a BillResolutionFailed. The test therefore proves that
    // parse validation runs before any DB interaction.
    val badEntity = BillDO.hasPlaceholder.placeholder("not-a-valid-key")
    val outcome   = repo.insertIfNotExists(badEntity).attempt.unsafeRunSync()
    outcome match {
      case Left(e: BillResolutionFailed) =>
        e.billNaturalKey shouldBe "not-a-valid-key"
      case other => fail(s"expected Left(BillResolutionFailed), got $other")
    }
  }

}
