<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/alloydb-persistence.md -->

# Pattern: AlloyDB Persistence

AlloyDB is PostgreSQL-compatible; Doobie HikariCP transactor works without modification. pgvector extension enables embedding storage and cosine similarity queries.

## When To Use
- Any AlloyDB read/write operation
- Setting up the Doobie transactor (once at app startup)
- Storing or querying vector embeddings with pgvector
- Replacing any former Firestore persistence call

## Source Files
- `docs/templates/skeletons/alloydb-repository.scala` — AlloyDB-specific patterns (pgvector)
- `docs/templates/skeletons/doobie-repository.scala` — General Doobie repository pattern

## Transactor Setup

```scala
// File: bill-identifier/src/main/scala/db/AlloyDbTransactor.scala

package db

import cats.effect.{Async, Resource}

import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor

import scala.concurrent.ExecutionContext

// Resource[F, Transactor[F]] is canonical Doobie pattern; ensures connection pool closes on shutdown.
// AlloyDB connection URL: jdbc:postgresql://<INSTANCE_IP>:5432/<DB_NAME>
// AlloyDB is wire-compatible with PostgreSQL — no special JDBC driver needed.
object AlloyDbTransactor {
  def make[F[_]: Async](
    jdbcUrl: String,
    username: String,
    password: String,
    maxPoolSize: Int
  ): Resource[F, Transactor[F]] =
    HikariTransactor.newHikariTransactor[F](
      driverClassName = "org.postgresql.Driver",
      url             = jdbcUrl,
      user            = username,
      pass            = password,
      connectEC       = ExecutionContext.global  // Use bounded thread pool in production
    )
}
```

## Basic Repository Pattern

```scala
// All Doobie queries run in ConnectionIO and are lifted into F via the Transactor.
// Auto-derived Read/Write instances eliminate boilerplate for case class mapping.

case class BillDO(billId: String, congress: Int, title: String, updateDate: String)

object BillRepository {
  // doobie.Meta derivation handles basic types automatically; provide explicit Meta[MyEnum] for enums.

  def upsert[F[_]: Async](bill: BillDO)(xa: Transactor[F]): F[Unit] =
    sql"""
      INSERT INTO bills (bill_id, congress, title, update_date)
      VALUES (${bill.billId}, ${bill.congress}, ${bill.title}, ${bill.updateDate})
      ON CONFLICT (bill_id) DO UPDATE
        SET title       = EXCLUDED.title,
            update_date = EXCLUDED.update_date
    """.update.run.transact(xa).void

  def findById[F[_]: Async](billId: String)(xa: Transactor[F]): F[Option[BillDO]] =
    sql"SELECT bill_id, congress, title, update_date FROM bills WHERE bill_id = $billId"
      .query[BillDO]
      .option
      .transact(xa)
}
```

## pgvector — Embedding Storage and Similarity Search

pgvector adds native 'vector' column type to PostgreSQL/AlloyDB. Store embeddings as TEXT (JSON array) and cast to vector in SQL.
Requires: `CREATE EXTENSION IF NOT EXISTS vector;` and `CREATE INDEX ON bill_analyses USING ivfflat (embedding vector_cosine_ops)`

```scala
object BillAnalysisRepository {
  // Store analysis result with embedding (1536-dim for text-embedding-3-small, 768-dim for Gemini)
  def insertWithEmbedding[F[_]: Async](
    analysisId: String,
    billId: String,
    pass1: io.circe.Json,
    embedding: Vector[Float]
  )(xa: Transactor[F]): F[Unit] = {
    val embeddingStr = embedding.mkString("[", ",", "]")
    sql"""
      INSERT INTO bill_analyses (analysis_id, bill_id, pass1, embedding, analyzed_at)
      VALUES ($analysisId, $billId, $pass1, $embeddingStr::vector, NOW())
    """.update.run.transact(xa).void
  }

  // Cosine similarity search — find bills closest to user profile embedding.
  // <=> operator is pgvector cosine distance (lower = more similar).
  def findSimilarBills[F[_]: Async](
    userEmbedding: Vector[Float],
    limit: Int
  )(xa: Transactor[F]): F[List[(String, Double)]] = {
    val embeddingStr = userEmbedding.mkString("[", ",", "]")
    sql"""
      SELECT bill_id, embedding <=> $embeddingStr::vector AS distance
      FROM bill_analyses
      ORDER BY distance
      LIMIT $limit
    """.query[(String, Double)].to[List].transact(xa)
  }
}
```

## Key Rules

| Rule | Rationale |
|------|-----------|
| Use `HikariTransactor` via `Resource[F, _]` | Connection pool lifecycle managed by Cats Effect resource |
| Doobie handles blocking I/O internally | Do NOT wrap Doobie calls in `Async[F].blocking` — redundant |
| Use `ON CONFLICT DO UPDATE` for upserts | AlloyDB is PostgreSQL — standard upsert syntax applies |
| Store embeddings as `vector` column type | pgvector `<=>` operator enables efficient cosine similarity |
| Use `transact(xa)` to lift `ConnectionIO` into `F` | All queries compose in ConnectionIO |
| Table names from `Tables` constants in pipeline-models | Single source of truth across all repos |
| Auto-derived `Read[T]`/`Write[T]` for case class mapping | Explicit `Meta` instances for enums only |

## How to Create a New AlloyDB Repository

```scala
// 1. Define domain object (in models/ sub-project)
case class VoteDO(voteId: String, billId: String, chamber: String, result: String)

// 2. Write the repository (in app/ sub-project)
object VoteRepository {
  def upsert[F[_]: Async](vote: VoteDO)(xa: Transactor[F]): F[Unit] =
    sql"""
      INSERT INTO votes (vote_id, bill_id, chamber, result)
      VALUES (${vote.voteId}, ${vote.billId}, ${vote.chamber}, ${vote.result})
      ON CONFLICT (vote_id) DO UPDATE
        SET result = EXCLUDED.result
    """.update.run.transact(xa).void

  def findByBillId[F[_]: Async](billId: String)(xa: Transactor[F]): F[List[VoteDO]] =
    sql"SELECT vote_id, bill_id, chamber, result FROM votes WHERE bill_id = $billId"
      .query[VoteDO]
      .to[List]
      .transact(xa)
}
```