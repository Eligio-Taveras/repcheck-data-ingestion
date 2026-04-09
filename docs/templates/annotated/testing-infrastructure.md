# Testing Infrastructure — Annotated Reference

## Pattern Summary

RepCheck uses a layered testing strategy: unit tests with MockitoScala, HTTP simulation with WireMock, real infrastructure with AlloyDB Omni (Docker), and end-to-end tests against a shared GCP dev environment. Test isolation is achieved through ephemeral namespaces — each test run generates a unique prefix for all cloud resources and cleans up after itself.

## When to Use This Guide

- Setting up tests for a new repository or module
- Adding integration tests that need AlloyDB, Pub/Sub, or PostgreSQL
- Writing end-to-end tests that verify full pipeline flows
- Understanding how tests avoid colliding with each other

## Source Files

```
docs/templates/annotated/test-patterns.md           ← Test scaffolds and assertion patterns
docs/templates/skeletons/test-templates.scala        ← Copy-and-fill test skeletons
docs/templates/skeletons/docker-compose-local-dev.yml ← Emulator stack
docs/templates/skeletons/github-actions-bug-on-failure.yml ← Auto-bug filing
```

---

## Testing Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Unit mocking | MockitoScala | Mock traits and interfaces (e.g., `AlloyDbRepository[F]`) |
| HTTP simulation | WireMock | Simulate Congress.gov API, LLM provider responses, failure scenarios |
| Real infrastructure | DockerPostgresSpec (custom Docker CLI) | Spin up pgvector PostgreSQL per test suite, apply migrations |
| Assertions | ScalaTest (AnyFlatSpec + Matchers) | BDD-style test structure with rich matchers |
| E2E | ScalaTest tags | Full pipeline verification against GCP dev project |

### MockitoScala Usage

```scala
// build.sbt dependency
"org.mockito" %% "mockito-scala-scalatest" % "PLACEHOLDER_VERSION" % Test
```

```scala
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar

class BillPipelineSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  val mockRepo = mock[AlloyDbRepository[IO]]
  // ^^^ Why MockitoScala: familiar API, good IDE support, works well with
  //     tagless final traits. Each mock is scoped to the test class.

  when(mockRepo.upsert(any[BillDO])).thenReturn(IO.unit)
  // ^^^ Stub behavior. MockitoScala integrates with Cats Effect IO —
  //     return IO values directly.

  "BillPipeline" should "persist fetched bills" in {
    // ... test body
    verify(mockRepo, times(3)).upsert(any[BillDO])
  }
}
```

### WireMock Usage

```scala
// build.sbt dependency
"org.wiremock" % "wiremock-standalone" % "PLACEHOLDER_VERSION" % Test
```

```scala
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._

class CongressApiSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val wireMock = new WireMockServer(0)  // random port
  // ^^^ Why random port: avoids port conflicts when tests run in parallel

  override def beforeAll(): Unit = {
    wireMock.start()
    // Stub Congress.gov bill list endpoint
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .willReturn(aResponse()
          .withStatus(200)
          .withBody(loadFixture("congress-bills-response.json")))
    )
    // Stub timeout scenario for retry testing
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/timeout"))
        .willReturn(aResponse()
          .withFixedDelay(30000))  // 30s delay triggers timeout
    )
  }

  override def afterAll(): Unit = wireMock.stop()
}
```

### AlloyDB Omni via DockerPostgresSpec

The project uses a custom Docker CLI wrapper instead of `testcontainers-scala` (which has a docker-java API version incompatibility with Docker 29+). The `DockerPostgresSpec` trait in `db-migrations` manages the full lifecycle: starts a `google/alloydbomni:16.8.0` container (AlloyDB Omni — the same engine used in staging/prod, with pgvector built in), waits for readiness (with JDBC retry to handle the pg_isready/JDBC gap), applies Liquibase migrations, and tears down on suite completion.

```scala
// build.sbt — reuse the trait from db-migrations test scope
lazy val billIdentifier = (project in file("bill-identifier"))
  .dependsOn(govApis, dbMigrations % "test->test")
  // ^^^ "test->test" makes db-migrations test classes available in
  //     bill-identifier's test classpath. This is how other modules
  //     reuse DockerPostgresSpec without duplicating container logic.
```

```scala
import doobie.util.transactor.Transactor
import repcheck.db.migrations.DockerPostgresSpec

class DoobieRepositorySpec extends AnyFlatSpec
    with Matchers
    with DockerPostgresSpec {
  // ^^^ DockerPostgresSpec handles beforeAll/afterAll:
  //     - Starts an AlloyDB Omni container with random port
  //     - Applies all Liquibase migrations (full schema)
  //     - Exposes jdbcUrl, getConnection for test use
  //     - Removes the container in afterAll

  private lazy val xa: Transactor[IO] =
    Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver",
      url = jdbcUrl,          // provided by DockerPostgresSpec
      user = "test",
      password = "test",
      logHandler = None,
    )
  // ^^^ Use fromDriverManager (not fromConnection) — it manages
  //     connection lifecycle per transaction, avoiding stale
  //     connection issues across tests.
}
```

---

## Local Dev Environment

### Docker Compose Stack

For interactive local development (running the full application locally), use the Docker Compose stack:

```bash
docker-compose -f docker-compose-local-dev.yml up -d
```

This starts:
- **AlloyDB Omni (google/alloydbomni)** on `localhost:5432`
- **Pub/Sub emulator** on `localhost:8085`

### Environment Variable Wiring

Applications detect emulators via standard environment variables:

```bash
# Set these when running locally (or in application-local.conf)
export DATABASE_URL=jdbc:postgresql://localhost:5432/repcheck
export PUBSUB_EMULATOR_HOST=localhost:8085
```

When `PUBSUB_EMULATOR_HOST` is set, the Pub/Sub client automatically connects to the emulator instead of real GCP. AlloyDB connection uses standard JDBC/Doobie.

### Config-Driven Targeting

Each application has environment-specific config files:

```
src/main/resources/
├── application.conf            ← Production defaults
├── application-local.conf      ← Local dev (emulators)
└── application-test.conf       ← Test (AlloyDB Omni Docker or dev GCP project)
```

```hocon
# application-local.conf
alloydb {
  url = "jdbc:postgresql://localhost:5432/repcheck"
  user = "repcheck"
  password = "localdev"
}

pubsub {
  project-id = "repcheck-local"
  # No credentials needed — emulator ignores auth
}
```

### Non-Emulatable Cloud Resources

For GCP services that have no local emulator (e.g., Cloud Scheduler, Artifact Registry), use the shared dev GCP project:

- Project: `repcheck-dev`
- Resources namespaced per developer: prefix with developer ID or use separate service accounts
- Config in `application-local.conf` points to dev project for these specific services

---

## Test Isolation — Ephemeral Namespaces

### Problem
Multiple developers and CI runs may hit the same GCP dev project. Without isolation, test data collides.

### Solution
Each test run generates a unique schema prefix. All AlloyDB tables and Pub/Sub topics created during the test use this prefix. Cleanup runs in `afterAll()`.

### Implementation

```scala
import java.util.UUID

trait EphemeralNamespace {

  // Generate a unique prefix for this test run
  val testPrefix: String = s"test_${UUID.randomUUID().toString.take(8)}_"
  // ^^^ Example: "test_a1b2c3d4_"
  //     Short enough to be readable in logs, unique enough to avoid collisions.
  //     Uses underscores — PostgreSQL table names can't contain hyphens.

  // Prefix a table or topic name
  def namespaced(name: String): String = s"$testPrefix$name"
  // ^^^ "bills" becomes "test_a1b2c3d4_bills"
  //     "bill-events" becomes "test_a1b2c3d4_bill-events"

  // Override the Tables object constants for tests
  def namespacedTables: Map[String, String] = Map(
    "bills"     -> namespaced("bills"),
    "votes"     -> namespaced("votes"),
    "members"   -> namespaced("members"),
    "amendments" -> namespaced("amendments"),
    "analyses"  -> namespaced("analyses"),
    "scores"    -> namespaced("scores"),
  )

  // Cleanup: drop test tables and delete Pub/Sub resources with this prefix
  def cleanupNamespace(xa: Transactor[IO]): IO[Unit] = {
    for {
      _ <- sql"DROP SCHEMA IF EXISTS #${testPrefix} CASCADE".update.run.transact(xa).void
      _ <- deletePubSubTopics(testPrefix)
      _ <- deletePubSubSubscriptions(testPrefix)
    } yield ()
  }
  // ^^^ Called in afterAll(). Drops ONLY schemas/tables with this run's prefix.
  //     Safe even if cleanup fails — orphaned prefixed tables don't affect
  //     other test runs. A periodic cleanup job can sweep old prefixes.
}
```

### Usage in Tests

```scala
class BillIngestionIntegrationSpec extends AnyFlatSpec
    with Matchers
    with EphemeralNamespace
    with BeforeAndAfterAll {

  // All AlloyDB operations use namespaced table names
  val billsTable = namespaced("bills")
  val billEventsTopic = namespaced("bill-events")

  override def afterAll(): Unit = {
    cleanupNamespace(xa).unsafeRunSync()
    super.afterAll()
  }

  "BillIngestion" should "persist bills to namespaced table" in {
    // Test writes to "test_a1b2c3d4_bills", not "bills"
    // No collision with other test runs or real data
  }
}
```

### CI Isolation

CI runs use the same `EphemeralNamespace` trait. The CI workflow sets:
```yaml
env:
  GOOGLE_CLOUD_PROJECT: repcheck-dev
  TEST_NAMESPACE_PREFIX: ci-${{ github.run_id }}-
```

This ensures CI runs are isolated from each other and from developer test runs.

---

## E2E Tests

### Structure

E2E tests live in the same SBT modules as unit tests but are tagged to run separately.

```scala
import org.scalatest.Tag

object E2ETest extends Tag("com.repcheck.tags.E2ETest")
```

```scala
class FullPipelineE2ESpec extends AnyFlatSpec with Matchers with EphemeralNamespace {

  "Full pipeline" should "ingest, analyze, and score" taggedAs E2ETest in {
    // 1. Trigger bill ingestion against dev Congress.gov (or WireMock)
    // 2. Verify bills persisted to AlloyDB (namespaced table)
    // 3. Verify bill.text.available event published to Pub/Sub (namespaced topic)
    // 4. Trigger analysis pipeline
    // 5. Verify analysis stored
    // 6. Trigger scoring
    // 7. Verify scores computed
  }
}
```

### SBT Configuration

```scala
// build.sbt — exclude E2E from normal test runs
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "com.repcheck.tags.E2ETest")
```

### Running Tests

```bash
# Unit + integration tests only (default — E2E excluded)
sbt test

# E2E tests only
sbt "testOnly -- -n com.repcheck.tags.E2ETest"

# All tests including E2E
sbt "testOnly -- --include-all"
```

### E2E Config

E2E tests use `application-e2e.conf` pointing to the dev GCP project:

```hocon
# application-e2e.conf
alloydb {
  url = ${ALLOYDB_URL}
  user = ${ALLOYDB_USER}
  password = ${ALLOYDB_PASSWORD}
}

pubsub {
  project-id = "repcheck-dev"
}

congress-api {
  base-url = "https://api.congress.gov"
  api-key = ${CONGRESS_GOV_API_KEY}
}
```

---

## Auto-Bug Filing on CI Failure

When CI tests fail, a GitHub Action automatically creates a GitHub Issue.

### Behavior:
1. Test job fails → bug-filing step runs
2. Creates a GitHub Issue with:
   - Title: `CI Failure: {test-suite-name} on {branch}`
   - Body: error log (last 50 lines), commit SHA, link to CI run
   - Labels: `bug/ci-failure`, `automated`
3. On subsequent passing CI run → searches for open `bug/ci-failure` issues matching the context → closes them automatically

### Why:
- Failed tests don't get lost in CI notification noise
- Issues create a trackable backlog item
- Auto-close prevents stale issues when the fix lands

See `docs/templates/skeletons/github-actions-bug-on-failure.yml` for the workflow template.

---

## Cross-References

- **Test patterns (assertions, negative testing)**: `docs/templates/annotated/test-patterns.md`
- **Test skeletons (copy-and-fill)**: `docs/templates/skeletons/test-templates.scala`
- **Docker Compose stack**: `docs/templates/skeletons/docker-compose-local-dev.yml`
- **Deployment architecture**: `docs/templates/annotated/deployment-architecture.md`
- **Behavioral specs (what to test)**: `docs/architecture/BEHAVIORAL_SPECS.md`
