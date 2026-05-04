package repcheck.members.common.persistence

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.LogHandler
import doobie.implicits._
import doobie.util.log.LogEvent
import doobie.util.transactor.Transactor

import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.errors.InvalidBioguideId

class MemberRepositoryUpsertPlaceholderSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieMemberRepository

  private def withCountingTransactor[A](body: (Transactor[IO], AtomicInteger) => A): A = {
    val counter = new AtomicInteger(0)
    val handler = new LogHandler[IO] {
      override def run(event: LogEvent): IO[Unit] = IO.delay { val _ = counter.incrementAndGet() }
    }
    val dbName = s"upsert-placeholder-${java.util.UUID.randomUUID().toString.take(8)}"
    val xa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = s"jdbc:h2:mem:$dbName;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = Some(handler),
    )
    val _ = sql"""CREATE TABLE members (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    natural_key VARCHAR(255) NOT NULL UNIQUE
                  )""".update.run.transact(xa).unsafeRunSync()
    counter.set(0)
    body(xa, counter)
  }

  private def assertRejected(input: String): Assertion =
    withCountingTransactor { (xa, counter) =>
      repo.upsertPlaceholder(input).transact(xa).attempt.unsafeRunSync() match {
        case Left(_: InvalidBioguideId) => counter.get() shouldBe 0
        case other                      => fail(s"expected Left(InvalidBioguideId), got $other")
      }
    }

  "upsertPlaceholder" should "raise InvalidBioguideId without issuing any SQL for an empty string" in {
    assertRejected("")
  }

  it should "raise InvalidBioguideId without issuing any SQL for a 3-char alpha code" in {
    assertRejected("abc")
  }

  it should "raise InvalidBioguideId for a key with only 5 digits (A00037)" in {
    assertRejected("A00037")
  }

  it should "raise InvalidBioguideId for a key with a lowercase prefix (a000370)" in {
    assertRejected("a000370")
  }

  it should "raise InvalidBioguideId for a key with 7 digits (A0003700)" in {
    assertRejected("A0003700")
  }

  it should "raise InvalidBioguideId carrying the malformed value in the message" in {
    val outcome = repo.upsertPlaceholder("bogus").transact(stubXa).attempt.unsafeRunSync()
    outcome match {
      case Left(e: InvalidBioguideId) =>
        val _ = e.value shouldBe "bogus"
        e.getMessage should include("bogus")
      case other => fail(s"expected Left(InvalidBioguideId), got $other")
    }
  }

  // Used purely to drive the ConnectionIO; validation fails before any SQL is sent so the URL never matters.
  private lazy val stubXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:upsert-placeholder-stub;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  "DoobieMemberRepository" should "produce a ConnectionIO for a valid bioguide id" in {
    val cio = repo.upsertPlaceholder("Z999999")
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
