// =============================================================================
// RepCheck Skeleton: AlloyDB Repository (pgvector + Doobie)
// Repo: repcheck-pipeline-models (shared library)
// =============================================================================
//
// PURPOSE: AlloyDB-specific repository patterns — transactor setup, table name
// constants, upsert patterns, and pgvector embedding queries.
//
// KEY DECISIONS (from Q&A):
// - AlloyDB is PostgreSQL-compatible; use standard Doobie HikariCP transactor
// - Table name constants in pipeline-models (single source of truth)
// - Auto-derived Read/Write for case class mapping (no toPojo needed)
// - pgvector stored as TEXT cast to ::vector in SQL
// - Uses retry wrapper for transient connection errors
// =============================================================================

package repcheck.pipeline.models.db

import cats.effect.{Async, Resource}
import cats.syntax.all.*

import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import doobie.util.transactor.Transactor

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

import repcheck.pipeline.models.retry.{ErrorClassifier, RetryConfig, RetryWrapper}

// ---------------------------------------------------------------------------
// Table Name Constants — single source of truth for all repos
// ---------------------------------------------------------------------------

object Tables {
  val Bills              = "bills"
  val Members            = "members"
  val Votes              = "votes"
  val VotePositions      = "vote_positions"
  val VoteHistory        = "vote_history"
  val Amendments         = "amendments"
  val BillAnalyses       = "bill_analyses"
  val Scores             = "scores"
  val ScoreHistory       = "score_history"
  val Users              = "users"
  val UserPreferences    = "user_preferences"
  val PipelineRuns       = "pipeline_runs"
  val ProcessingResults  = "processing_results"
}

// ---------------------------------------------------------------------------
// AlloyDB Transactor — created once at app startup
// ---------------------------------------------------------------------------

object AlloyDbTransactor {

  final case class AlloyDbConfig(
    jdbcUrl: String,       // jdbc:postgresql://<IP>:5432/<dbname>
    username: String,
    password: String,
    maxPoolSize: Int = 10
  )

  /** Create a HikariCP transactor as a Resource[F, Transactor[F]].
    *
    * Resource ensures the connection pool is shut down on app termination.
    * AlloyDB is wire-compatible with PostgreSQL — use the standard pg driver.
    */
  def make[F[_]: Async](config: AlloyDbConfig): Resource[F, Transactor[F]] =
    HikariTransactor.newHikariTransactor[F](
      driverClassName = "org.postgresql.Driver",
      url             = config.jdbcUrl,
      user            = config.username,
      pass            = config.password,
      connectEC       = ExecutionContext.global
    )
}

// ---------------------------------------------------------------------------
// pgvector Embedding Queries
// ---------------------------------------------------------------------------

/** AlloyDB-specific repository for bill analysis with pgvector support.
  *
  * Requires:
  *   CREATE EXTENSION IF NOT EXISTS vector;
  *   ALTER TABLE bill_analyses ADD COLUMN embedding vector(1536);
  *   CREATE INDEX ON bill_analyses USING ivfflat (embedding vector_cosine_ops);
  */
trait BillAnalysisRepository[F[_]] {

  /** Insert a bill analysis result with its semantic embedding. */
  def insert(
    analysisId: String,
    billId: String,
    pass1Json: String,   // JSONB as serialized string
    embedding: Vector[Float]
  ): F[Unit]

  /** Find the N most similar bills to a query embedding using cosine similarity. */
  def findSimilar(
    queryEmbedding: Vector[Float],
    limit: Int
  ): F[List[(String, Double)]]  // (billId, distance) — lower distance = more similar

  /** Fetch the latest analysis for a bill. */
  def findLatest(billId: String): F[Option[BillAnalysisDO]]
}

case class BillAnalysisDO(
  analysisId: String,
  billId: String,
  pass1: Option[String],  // JSONB stored as text
  pass2: Option[String],
  pass3: Option[String],
  analyzedAt: java.time.Instant
)

object BillAnalysisRepository {

  def make[F[_]: Async](
    xa: Transactor[F],
    classifier: ErrorClassifier,
    retry: RetryConfig = RetryConfig(
      maxRetries      = 3,
      initialBackoff  = 10.millis,
      maxBackoff      = 60.seconds,
      timeout         = 30.seconds
    )
  ): BillAnalysisRepository[F] = new BillAnalysisRepository[F] {

    def insert(
      analysisId: String,
      billId: String,
      pass1Json: String,
      embedding: Vector[Float]
    ): F[Unit] = {
      val embStr = embedding.mkString("[", ",", "]")
      val op: F[Unit] =
        sql"""
          INSERT INTO bill_analyses (analysis_id, bill_id, pass1, embedding, analyzed_at)
          VALUES ($analysisId, $billId, $pass1Json::jsonb, $embStr::vector, NOW())
        """.update.run.transact(xa).void

      RetryWrapper.withRetry[F, Unit](retry, classifier, "alloydb-insert-analysis")(op)
    }

    def findSimilar(
      queryEmbedding: Vector[Float],
      limit: Int
    ): F[List[(String, Double)]] = {
      val embStr = queryEmbedding.mkString("[", ",", "]")
      val op: F[List[(String, Double)]] =
        sql"""
          SELECT bill_id, embedding <=> $embStr::vector AS distance
          FROM bill_analyses
          ORDER BY distance
          LIMIT $limit
        """.query[(String, Double)].to[List].transact(xa)

      RetryWrapper.withRetry[F, List[(String, Double)]](
        retry, classifier, "alloydb-similarity-search"
      )(op)
    }

    def findLatest(billId: String): F[Option[BillAnalysisDO]] = {
      val op: F[Option[BillAnalysisDO]] =
        sql"""
          SELECT analysis_id, bill_id, pass1::text, pass2::text, pass3::text, analyzed_at
          FROM bill_analyses
          WHERE bill_id = $billId
          ORDER BY analyzed_at DESC
          LIMIT 1
        """.query[BillAnalysisDO].option.transact(xa)

      RetryWrapper.withRetry[F, Option[BillAnalysisDO]](
        retry, classifier, "alloydb-find-latest-analysis"
      )(op)
    }
  }
}

// ---------------------------------------------------------------------------
// Generic Upsert Helper
// ---------------------------------------------------------------------------

/** Common pattern for legislative entity upserts (bills, votes, members, amendments).
  *
  * All legislative entities use ON CONFLICT DO UPDATE with their natural key.
  * The `Fragment` approach allows callers to compose SQL dynamically while
  * remaining parameterized (no string interpolation = no SQL injection).
  */
object UpsertHelper {

  /** Execute an upsert query within the given transactor, with retry. */
  def upsert[F[_]: Async](
    query: doobie.Update0,
    tableName: String,
    xa: Transactor[F],
    classifier: ErrorClassifier,
    retry: RetryConfig
  ): F[Unit] = {
    val op: F[Unit] = query.run.transact(xa).void
    RetryWrapper.withRetry[F, Unit](retry, classifier, s"alloydb-upsert($tableName)")(op)
  }
}
