// =============================================================================
// RepCheck Skeleton: Doobie Repository Pattern
// Repo: repcheck-scoring-engine (or any repo needing AlloyDB)
// =============================================================================
//
// PURPOSE: Per-entity repository trait with auto-derived Read/Write instances.
// AlloyDB (PostgreSQL-compatible) accessed via Doobie with Cats Effect integration.
//
// KEY DECISIONS (from Q&A):
// - One repository trait per entity (UserRepository, PreferenceRepository, etc.)
// - Auto-derived Read/Write from case classes (minimal boilerplate)
// - Transactor[F] injection
// - ConnectionIO queries transacted into F
// - Uses retry wrapper for connection-level errors
// =============================================================================

package repcheck.db.repository

import cats.effect.{Async, Resource}
import cats.syntax.all.*

import doobie.*
import doobie.implicits.*
import doobie.util.transactor.Transactor

import java.util.UUID

// ---------------------------------------------------------------------------
// Domain Objects (would live in the repo's models/ sub-project)
// ---------------------------------------------------------------------------

/** Example: User profile domain object. */
final case class UserDO(
    userId: UUID,
    displayName: String,
    email: String,
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

/** Example: User political preference domain object. */
final case class UserPreferenceDO(
    preferenceId: UUID,
    userId: UUID,
    topic: String,
    stance: String,        // e.g., "support", "oppose", "neutral"
    importance: Int,        // 1-10 scale
    updatedAt: java.time.Instant
)

// ---------------------------------------------------------------------------
// Repository Trait — one per entity
// ---------------------------------------------------------------------------

/** User repository — all user-related AlloyDB queries.
  *
  * Tagless final: F[_] is the effect type (IO in production).
  */
trait UserRepository[F[_]] {
  def findById(userId: UUID): F[Option[UserDO]]
  def findByEmail(email: String): F[Option[UserDO]]
  def upsert(user: UserDO): F[UserDO]
  def delete(userId: UUID): F[Unit]
}

/** User preference repository — all preference-related queries. */
trait PreferenceRepository[F[_]] {
  def findByUserId(userId: UUID): F[List[UserPreferenceDO]]
  def findByUserAndTopic(userId: UUID, topic: String): F[Option[UserPreferenceDO]]
  def upsert(pref: UserPreferenceDO): F[UserPreferenceDO]
  def deleteByUserId(userId: UUID): F[Unit]
}

// ---------------------------------------------------------------------------
// Doobie Implementation — UserRepository
// ---------------------------------------------------------------------------

object UserRepository {

  /** Create a UserRepository backed by Doobie.
    *
    * @param xa Transactor — manages connection pool and transaction boundaries
    */
  def make[F[_]: Async](xa: Transactor[F]): UserRepository[F] =
    new UserRepository[F] {

      def findById(userId: UUID): F[Option[UserDO]] =
        sql"""
          SELECT user_id, display_name, email, created_at, updated_at
          FROM users
          WHERE user_id = $userId
        """
          .query[UserDO]
          .option
          .transact(xa)

      def findByEmail(email: String): F[Option[UserDO]] =
        sql"""
          SELECT user_id, display_name, email, created_at, updated_at
          FROM users
          WHERE email = $email
        """
          .query[UserDO]
          .option
          .transact(xa)

      def upsert(user: UserDO): F[UserDO] =
        sql"""
          INSERT INTO users (user_id, display_name, email, created_at, updated_at)
          VALUES (${user.userId}, ${user.displayName}, ${user.email}, ${user.createdAt}, ${user.updatedAt})
          ON CONFLICT (user_id)
          DO UPDATE SET
            display_name = EXCLUDED.display_name,
            email = EXCLUDED.email,
            updated_at = EXCLUDED.updated_at
        """
          .update
          .run
          .transact(xa)
          .as(user)

      def delete(userId: UUID): F[Unit] =
        sql"DELETE FROM users WHERE user_id = $userId"
          .update
          .run
          .transact(xa)
          .void
    }
}

// ---------------------------------------------------------------------------
// Doobie Implementation — PreferenceRepository
// ---------------------------------------------------------------------------

object PreferenceRepository {

  def make[F[_]: Async](xa: Transactor[F]): PreferenceRepository[F] =
    new PreferenceRepository[F] {

      def findByUserId(userId: UUID): F[List[UserPreferenceDO]] =
        sql"""
          SELECT preference_id, user_id, topic, stance, importance, updated_at
          FROM user_preferences
          WHERE user_id = $userId
          ORDER BY importance DESC
        """
          .query[UserPreferenceDO]
          .to[List]
          .transact(xa)

      def findByUserAndTopic(
          userId: UUID,
          topic: String
      ): F[Option[UserPreferenceDO]] =
        sql"""
          SELECT preference_id, user_id, topic, stance, importance, updated_at
          FROM user_preferences
          WHERE user_id = $userId AND topic = $topic
        """
          .query[UserPreferenceDO]
          .option
          .transact(xa)

      def upsert(pref: UserPreferenceDO): F[UserPreferenceDO] =
        sql"""
          INSERT INTO user_preferences (preference_id, user_id, topic, stance, importance, updated_at)
          VALUES (${pref.preferenceId}, ${pref.userId}, ${pref.topic}, ${pref.stance}, ${pref.importance}, ${pref.updatedAt})
          ON CONFLICT (preference_id)
          DO UPDATE SET
            stance = EXCLUDED.stance,
            importance = EXCLUDED.importance,
            updated_at = EXCLUDED.updated_at
        """
          .update
          .run
          .transact(xa)
          .as(pref)

      def deleteByUserId(userId: UUID): F[Unit] =
        sql"DELETE FROM user_preferences WHERE user_id = $userId"
          .update
          .run
          .transact(xa)
          .void
    }
}

// ---------------------------------------------------------------------------
// Transactor Setup
// ---------------------------------------------------------------------------

object TransactorSetup {

  final case class AlloyDbConfig(
      host: String,
      port: Int = 5432,
      database: String,
      username: String,
      password: String
  )

  /** Create a Doobie Transactor from AlloyDB config.
    * Uses HikariCP connection pool under the hood.
    */
  def make[F[_]: Async](config: AlloyDbConfig): Resource[F, Transactor[F]] =
    // TODO: In production, use doobie.hikari.HikariTransactor for connection pooling
    //
    // HikariTransactor.newHikariTransactor[F](
    //   driverClassName = "org.postgresql.Driver",
    //   url = s"jdbc:postgresql://${config.host}:${config.port}/${config.database}",
    //   user = config.username,
    //   pass = config.password,
    //   connectEC = ExecutionContext.global  // or a dedicated connect pool
    // )
    Resource.pure(
      Transactor.fromDriverManager[F](
        driver = "org.postgresql.Driver",
        url = s"jdbc:postgresql://${config.host}:${config.port}/${config.database}",
        user = config.username,
        password = config.password
      )
    )
}
