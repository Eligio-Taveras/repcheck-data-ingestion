package repcheck.members.common.persistence

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import cats.effect.std.Supervisor
import cats.effect.unsafe.implicits.global
import cats.implicits._

import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.log.LogEvent
import doobie.{LogHandler, Transactor}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}

class DoobieMemberRepositoryUpsertPlaceholderSpec extends AnyFlatSpec with Matchers with TransactorFixture {

  private lazy val repo = new DoobieMemberRepository

  private def countMembers(bioguideId: String): Long =
    sql"SELECT COUNT(*) FROM members WHERE natural_key = $bioguideId"
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

  "upsertPlaceholder" should "insert a new placeholder row and return its id" taggedAs DockerRequired in {
    val id = repo.upsertPlaceholder("X000001").transact(xa).unsafeRunSync()
    val _  = id should be > 0L
    countMembers("X000001") shouldBe 1L
  }

  // Pinned to a counting LogHandler-backed transactor so the assertion is "exactly 1 doobie statement" — i.e. one
  // network roundtrip — rather than just "exactly 1 row inserted". This is the key contract of the single-SQL pattern:
  // the previous would-be `findByBioguideId` + insert if missing approach would emit 2 statements per call. Catching
  // that regression at the unit level isn't possible because H2 doesn't speak `ON CONFLICT ... DO UPDATE ... RETURNING`,
  // so the assertion lives here against the real AlloyDB Omni.
  it should "issue exactly one doobie statement per call (single roundtrip)" taggedAs DockerRequired in {
    val counter = new AtomicInteger(0)
    val handler = new LogHandler[IO] {
      override def run(event: LogEvent): IO[Unit] = IO.delay { val _ = counter.incrementAndGet() }
    }
    val countingXa: Transactor[IO] = Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver",
      url = containerInfo.jdbcUrl,
      user = containerInfo.user,
      password = containerInfo.password,
      logHandler = Some(handler),
    )

    val _ = repo.upsertPlaceholder("X000010").transact(countingXa).unsafeRunSync()
    val _ = counter.get() shouldBe 1

    counter.set(0)
    val _ = repo.upsertPlaceholder("X000010").transact(countingXa).unsafeRunSync()
    counter.get() shouldBe 1
  }

  it should "be idempotent — a second call with the same bioguide returns the same id" taggedAs DockerRequired in {
    val first  = repo.upsertPlaceholder("X000002").transact(xa).unsafeRunSync()
    val second = repo.upsertPlaceholder("X000002").transact(xa).unsafeRunSync()
    val _      = second shouldBe first
    countMembers("X000002") shouldBe 1L
  }

  it should "leave update_date NULL on the inserted placeholder row" taggedAs DockerRequired in {
    val _ = repo.upsertPlaceholder("X000003").transact(xa).unsafeRunSync()
    val updateDate = sql"SELECT update_date FROM members WHERE natural_key = 'X000003'"
      .query[Option[java.time.Instant]]
      .unique
      .transact(xa)
      .unsafeRunSync()
    updateDate shouldBe None
  }

  it should "produce exactly 1 row when called twice concurrently with the same bioguide" taggedAs DockerRequired in {
    val concurrentRuns = 10
    val program = Supervisor[IO](await = true).use { supervisor =>
      for {
        fibers <- (1 to concurrentRuns).toList.traverse { _ =>
          supervisor.supervise(repo.upsertPlaceholder("X000004").transact(xa))
        }
        ids <- fibers.traverse(_.joinWithNever)
      } yield ids
    }
    val ids = program.unsafeRunSync()
    val _   = ids.distinct.size shouldBe 1
    countMembers("X000004") shouldBe 1L
  }

  it should "preserve a fully populated existing row — placeholder upsert is a no-op overwrite for natural_key" taggedAs DockerRequired in {
    val originalId    = insertMember("X000005", firstName = "Real", lastName = "Person")
    val placeholderId = repo.upsertPlaceholder("X000005").transact(xa).unsafeRunSync()
    val _             = placeholderId shouldBe originalId

    val firstName = sql"SELECT first_name FROM members WHERE natural_key = 'X000005'"
      .query[Option[String]]
      .unique
      .transact(xa)
      .unsafeRunSync()
    firstName shouldBe Some("Real")
  }

}
