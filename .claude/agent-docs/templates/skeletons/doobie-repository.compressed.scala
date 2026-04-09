<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/doobie-repository.scala -->

```markdown
# RepCheck Skeleton: Doobie Repository Pattern

**Purpose:** Per-entity repository trait with auto-derived Read/Write instances for AlloyDB (PostgreSQL) via Doobie + Cats Effect.

**Key Decisions:**
- One repository trait per entity
- Auto-derived Read/Write from case classes
- Transactor[F] injection
- ConnectionIO queries transacted into F
- Retry wrapper for connection-level errors

---

## Domain Objects

```scala
final case class UserDO(
    userId: UUID,
    displayName: String,
    email: String,
    createdAt: java.time.Instant,
    updatedAt: java.time.Instant
)

final case class UserPreferenceDO(
    preferenceId: UUID,
    userId: UUID,
    topic: String,
    stance: String,        // e.g., "support", "oppose", "neutral"
    importance: Int,        // 1-10 scale
    updatedAt: java.time.Instant
)
```

---

## Repository Traits

```scala
trait UserRepository[F[_]] {
  def findById(userId: UUID): F[Option[UserDO]]
  def findByEmail(email: String): F[Option[UserDO]]
  def upsert(user: UserDO): F[UserDO]
  def delete(userId: UUID): F[Unit]
}

trait PreferenceRepository[F[_]] {
  def findByUserId(userId: UUID): F[List[UserPreferenceDO]]
  def findByUserAndTopic(userId: UUID, topic: String): F[Option[UserPreferenceDO]]
  def upsert(pref: UserPreferenceDO): F[UserPreferenceDO]
  def deleteByUserId(userId: UUID): F[Unit]
}
```

---

## UserRepository Implementation

```scala
object UserRepository {
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
```

---

## PreferenceRepository Implementation

```scala
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

      def findByUserAndTopic(userId: UUID, topic: String): F[Option[UserPreferenceDO]] =
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
```

---

## Transactor Setup

```scala
object TransactorSetup {
  final case class AlloyDbConfig(
      host: String,
      port: Int = 5432,
      database: String,
      username: String,
      password: String
  )

  def make[F[_]: Async](config: AlloyDbConfig): Resource[F, Transactor[F]] =
    // In production, use HikariTransactor for connection pooling
    Resource.pure(
      Transactor.fromDriverManager[F](
        driver = "org.postgresql.Driver",
        url = s"jdbc:postgresql://${config.host}:${config.port}/${config.database}",
        user = config.username,
        password = config.password
      )
    )
}
```

**Production Note:** Replace `Resource.pure` with `HikariTransactor.newHikariTransactor[F]` for connection pooling.
```