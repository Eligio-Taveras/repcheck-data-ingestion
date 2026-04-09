> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 8. AlloyDB Access (Doobie)

**Pattern**: Doobie with `ConnectionIO` for queries, `Transactor[F]` for execution. AlloyDB is the single unified database for all RepCheck data — legislative data, analysis results, scores, and user data. Supports pgvector for embeddings.

### Table Constants (in `repcheck-pipeline-models`)

```scala
object Tables {
  val Bills              = "bills"
  val Members            = "members"
  val Votes              = "votes"
  val VotePositions      = "vote_positions"
  val Amendments         = "amendments"
  val BillAnalyses       = "bill_analyses"
  val Scores             = "scores"
  val ScoreHistory       = "score_history"
  val Users              = "users"
  val UserPreferences    = "user_preferences"
  val PipelineRuns       = "pipeline_runs"
  val ProcessingResults  = "processing_results"
}
```

### Transactor Setup

```scala
import doobie.*
import doobie.implicits.*
import doobie.hikari.HikariTransactor
import cats.effect.{Async, Resource}

object AlloyDbTransactor {
  def make[F[_]: Async](url: String, user: String, password: String): Resource[F, HikariTransactor[F]] =
    HikariTransactor.newHikariTransactor[F](
      driverClassName = "org.postgresql.Driver",
      url = url,  // jdbc:postgresql://host:5432/repcheck
      user = user,
      pass = password,
      connectEC = scala.concurrent.ExecutionContext.global
    )
}
```

### Query Pattern

```scala
import doobie.*
import doobie.implicits.*
import doobie.implicits.javasql.*

// Read — auto-derived Read[T] from case class field order
def findBill(billId: String): ConnectionIO[Option[LegislativeBillDO]] =
  sql"""
    SELECT bill_id, congress, bill_type, title, origin_chamber,
           latest_action_text, latest_action_date, update_date, url
    FROM bills
    WHERE bill_id = $billId
  """.query[LegislativeBillDO].option

// Write — ON CONFLICT DO UPDATE for idempotent upserts
def upsertBill(bill: LegislativeBillDO): ConnectionIO[Int] =
  sql"""
    INSERT INTO bills (bill_id, congress, bill_type, title, origin_chamber,
                       latest_action_text, latest_action_date, update_date, url)
    VALUES (${bill.billId}, ${bill.congress}, ${bill.billType.value}, ${bill.title},
            ${bill.originChamber.value}, ${bill.latestActionText},
            ${bill.latestActionDate}, ${bill.updateDate}, ${bill.url})
    ON CONFLICT (bill_id) DO UPDATE SET
      latest_action_text = EXCLUDED.latest_action_text,
      latest_action_date = EXCLUDED.latest_action_date,
      update_date        = EXCLUDED.update_date
  """.update.run

// Execute against transactor
def saveBill[F[_]: Async](bill: LegislativeBillDO, xa: Transactor[F]): F[Unit] =
  upsertBill(bill).transact(xa).void
```

### pgvector Pattern (for embeddings)

```scala
import org.postgresql.util.PGobject

// Store a vector embedding
def upsertEmbedding(billId: String, embedding: Array[Float]): ConnectionIO[Int] = {
  val vectorStr = embedding.mkString("[", ",", "]")
  sql"""
    INSERT INTO bill_embeddings (bill_id, embedding)
    VALUES ($billId, $vectorStr::vector)
    ON CONFLICT (bill_id) DO UPDATE SET embedding = EXCLUDED.embedding
  """.update.run
}

// Cosine similarity search — <=> is cosine distance
def findSimilarBills(queryEmbedding: Array[Float], limit: Int): ConnectionIO[List[String]] = {
  val vectorStr = queryEmbedding.mkString("[", ",", "]")
  sql"""
    SELECT bill_id FROM bill_embeddings
    ORDER BY embedding <=> $vectorStr::vector
    LIMIT $limit
  """.query[String].to[List]
}
```

### Rules
- Table names always referenced via `Tables` constants — never hardcoded strings
- Use `ConnectionIO` for composable queries, `.transact(xa)` to execute
- Use `HikariTransactor` for connection pooling (via `Resource[F, HikariTransactor[F]]`)
- Doobie auto-derives `Read[T]` and `Write[T]` from case class field order — match column order in SELECT
- All upserts use `ON CONFLICT DO UPDATE` — never plain INSERT for legislative data
- pgvector extension must be enabled: `CREATE EXTENSION IF NOT EXISTS vector`
- AlloyDB uses standard PostgreSQL JDBC driver — no special socket factory needed
