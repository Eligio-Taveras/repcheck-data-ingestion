package repcheck.members.common.testing

import java.time.Instant

import cats.effect.IO

import doobie.Transactor
import doobie.implicits._
import doobie.postgres.implicits._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
 * Provides a shared AlloyDB Omni container and Doobie transactor for Docker-backed integration tests in
 * `members-common`. All suites share one container (via [[SharedDockerPostgres]]) and run sequentially (`Test /
 * parallelExecution := false` in `build.sbt`) to avoid cross-suite FK violations during cleanup.
 */
trait TransactorFixture extends BeforeAndAfterAll with BeforeAndAfterEach { self: Suite =>

  import cats.effect.unsafe.implicits.global

  protected lazy val containerInfo: PostgresContainerInfo = SharedDockerPostgres.info

  protected lazy val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = containerInfo.jdbcUrl,
    user = containerInfo.user,
    password = containerInfo.password,
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = containerInfo
  }

  override def afterEach(): Unit = {
    cleanTables()
    super.afterEach()
  }

  /** Insert a fully populated `members` row and return its auto-generated `id`. */
  protected def insertMember(
    bioguideId: String,
    firstName: String = "John",
    lastName: String = "Doe",
    party: String = "Democrat",
    state: String = "NY",
  ): Long = {
    val now = Instant.parse("2024-01-15T00:00:00Z")
    sql"""INSERT INTO members (
            natural_key, first_name, last_name, direct_order_name, inverted_order_name,
            honorific_name, birth_year, current_party, state, district,
            image_url, image_attribution, official_url, update_date
          ) VALUES (
            $bioguideId, $firstName, $lastName, ${Some(s"$firstName $lastName")},
            ${Some(s"$lastName, $firstName")}, ${Option.empty[String]}, ${Option.empty[Int]},
            $party::party_type, $state, ${Option.empty[Int]},
            ${Option.empty[String]}, ${Option.empty[String]}, ${Option.empty[String]}, $now
          )
          ON CONFLICT (natural_key) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            direct_order_name = EXCLUDED.direct_order_name,
            inverted_order_name = EXCLUDED.inverted_order_name,
            honorific_name = EXCLUDED.honorific_name,
            birth_year = EXCLUDED.birth_year,
            current_party = EXCLUDED.current_party,
            state = EXCLUDED.state,
            district = EXCLUDED.district,
            image_url = EXCLUDED.image_url,
            image_attribution = EXCLUDED.image_attribution,
            official_url = EXCLUDED.official_url,
            update_date = EXCLUDED.update_date,
            updated_at = NOW()
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()
  }

  /**
   * Insert a `lis_members` row using raw SQL so the fixture has no compile-time dependency on the LIS member Doobie
   * repository (which still lives in `lis-mapping-refresher`). Mirrors the INSERT shape used by
   * `DoobieLisMemberRepository.upsertByNaturalKey`.
   */
  protected def insertLisMember(
    naturalKey: String,
    firstName: Option[String] = Some("Test"),
    lastName: Option[String] = Some("Senator"),
    party: Option[String] = Some("D"),
    state: Option[String] = Some("NY"),
    lastVerified: Option[Instant] = Some(Instant.parse("2024-06-15T00:00:00Z")),
  ): Long =
    sql"""INSERT INTO lis_members (natural_key, first_name, last_name, party, state, last_verified)
          VALUES ($naturalKey, $firstName, $lastName, $party, $state, $lastVerified)
          ON CONFLICT (natural_key) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            party = EXCLUDED.party,
            state = EXCLUDED.state,
            last_verified = EXCLUDED.last_verified
          RETURNING id"""
      .query[Long]
      .unique
      .transact(xa)
      .unsafeRunSync()

  private def cleanTables(): Unit = {
    val _ = sql"""
      DELETE FROM member_term_history;
      DELETE FROM member_history;
      DELETE FROM member_party_history;
      DELETE FROM member_terms;
      DELETE FROM member_lis_mapping;
      DELETE FROM lis_members;
      DELETE FROM members;
    """.update.run.transact(xa).unsafeRunSync()
  }

}
