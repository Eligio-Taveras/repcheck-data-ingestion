<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/testing-infrastructure.md -->

# Testing Infrastructure — Annotated Reference

## Pattern Summary

RepCheck uses layered testing: unit tests with MockitoScala, HTTP simulation with WireMock, real infrastructure with AlloyDB Omni (Docker), and E2E tests against shared GCP dev environment. Test isolation via ephemeral namespaces — each test run generates unique resource prefix and cleans up after itself.

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
  // Familiar API, good IDE support, works with tagless final traits

  when(mockRepo.upsert(any[BillDO])).thenReturn(IO.unit)
  // MockitoScala integrates with Cats Effect IO — return IO values directly

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

  val wireMock = new WireMockServer(0)  // random port — avoids conflicts in parallel runs

  override def beforeAll(): Unit = {
    wireMock.start()
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill"))
        .willReturn(aResponse()
          .withStatus(200)
          .withBody(loadFixture("congress-bills-response.json")))
    )
    wireMock.stubFor(
      get(urlPathEqualTo("/v3/bill/timeout"))
        .willReturn(aResponse()
          .withFixedDelay(30000))  // 30s delay for retry testing
    )
  }

  override def afterAll(): Unit = wireMock.stop()
}
```

### AlloyDB Omni via DockerPostgresSpec

Custom Docker CLI wrapper (not testcontainers-scala — avoids docker-java incompatibility with Docker 29+). `DockerPostgresSpec` manages lifecycle: starts `google/alloydbomni:16.8.0` container with pgvector, waits for readiness (JDBC retry), applies Liquibase migrations, tears down on suite completion.

```scala
// build.sbt — reuse trait from db-migrations test scope
lazy val billIdentifier = (project in file("bill-identifier"))
  .dependsOn(govApis, dbMigrations % "test->test")
  // "test->test" makes db-migrations test classes available in bill-identifier's test classpath
```

```scala
import doobie.util.transactor.Transactor
import repcheck.db.migrations.DockerPostgresSpec

class DoobieRepositorySpec extends AnyFlatSpec
    with Matchers
    with DockerPostgresSpec {
  // DockerPostgresSpec handles beforeAll/afterAll: starts container, applies migrations, exposes jdbcUrl

  private lazy val xa: Transactor[IO] =
    Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver",
      url = jdbcUrl,          // provided by DockerPostgresSpec
      user = "test",
      password = "test",
      logHandler = None,
    )
  // Use fromDriverManager (not fromConnection) — manages connection lifecycle per transaction
}
```

---

## Local Dev Environment

### Docker Compose Stack

```bash
docker-compose -f docker-compose-local-dev.yml up -d
```

Starts AlloyDB Omni on `localhost:5432` and Pub/Sub emulator on `localhost:8085`.

### Environment Variable Wiring

```bash
export DATABASE_URL=jdbc:postgresql://localhost:5432/repcheck
export PUBSUB_EMULATOR_HOST=localhost:8085
```

When `PUBSUB_EMULATOR_HOST` is set, Pub/Sub client connects to emulator. AlloyDB uses standard JDBC/Doobie.

### Config-Driven Targeting

```
src/main/resources/
├── application.conf            ← Production defaults
├── application-local.conf      ← Local dev (emulators)
└── application-test.conf       ← Test (AlloyDB Omni Docker or dev GCP)
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
}
```

### Non-Emulatable Services

For GCP services without local emulator (Cloud Scheduler, Artifact Registry), use shared dev GCP project (`repcheck-dev`). Resources namespaced per developer. Config in `application-local.conf` points to dev for these services.

---

## Test Isolation — Ephemeral Namespaces

### Problem
Multiple developers and CI runs hit same GCP dev project. Without isolation, test data collides.

### Solution
Each test run generates unique schema prefix. All AlloyDB tables and Pub/Sub topics use this prefix. Cleanup runs in `afterAll()`.

### Implementation

```scala
import java.util.UUID

trait EphemeralNamespace {

  val testPrefix: String = s"test_${UUID.randomUUID().toString.take(8)}_"
  // Example: "test_a1b2c3d4_" — short, readable, avoids hyphens for PostgreSQL

  def namespaced(name: String): String = s"$testPrefix$name"
  // "bills" becomes "test_a1b2c3d4_bills"

  def namespacedTables: Map[String, String] = Map(
    "bills"     -> namespaced("bills"),
    "votes"     -> namespaced("votes"),
    "members"   -> namespaced("members"),
    "amendments" -> namespaced("amendments"),
    "analyses"  -> namespaced("analyses"),
    "scores"    -> namespaced("scores"),
  )

  def cleanupNamespace(xa: Transactor[IO]): IO[Unit] = {
    for {
      _ <- sql"DROP SCHEMA IF EXISTS #${testPrefix} CASCADE".update.run.transact(xa).void
      _ <- deletePubSubTopics(testPrefix)
      _ <- deletePubSubSubscriptions(testPrefix)
    } yield ()
  }
  // Called in afterAll(). Drops only schemas/tables with this run's prefix. Safe even if cleanup fails.
}
```

### Usage in Tests

```scala
class BillIngestionIntegrationSpec extends AnyFlatSpec
    with Matchers
    with EphemeralNamespace
    with BeforeAndAfterAll {

  val billsTable = namespaced("bills")
  val billEventsTopic = namespaced("bill-events")

  override def afterAll(): Unit = {
    cleanupNamespace(xa).unsafeRunSync()
    super.afterAll()
  }

  "BillIngestion" should "persist bills to namespaced table" in {
    // Test writes to "test_a1b2c3d4_bills", not "bills" — no collision
  }
}
```

### CI Isolation

CI uses same `EphemeralNamespace` trait with:
```yaml
env:
  GOOGLE_CLOUD_PROJECT: repcheck-dev
  TEST_NAMESPACE_PREFIX: ci-${{ github.run_id }}-
```

---

## E2E Tests

### Structure

E2E tests tagged to run separately:

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
sbt test                                              # Unit + integration (E2E excluded)
sbt "testOnly -- -n com.repcheck.tags.E2ETest"       # E2E only
sbt "testOnly -- --include-all"                       # All including E2E
```

### E2E Config

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

GitHub Action automatically creates issue when CI tests fail.

**Behavior:** Test job fails → bug-filing step runs → creates GitHub Issue with title `CI Failure: {test-suite-name} on {branch}`, body with error log (last 50 lines), commit SHA, CI run link. Labels: `bug/ci-failure`, `automated`. On subsequent passing CI run → searches for open `bug/ci-failure` issues → closes them.

**Why:** Failed tests don't get lost in notification noise. Issues create trackable backlog. Auto-close prevents stale issues when fix lands.

See `docs/templates/skeletons/github-actions-bug-on-failure.yml` for workflow template.

---

## Cross-References

- **Test patterns (assertions, negative testing)**: `docs/templates/annotated/test-patterns.md`
- **Test skeletons (copy-and-fill)**: `docs/templates/skeletons/test-templates.scala`
- **Docker Compose stack**: `docs/templates/skeletons/docker-compose-local-dev.yml`
- **Deployment architecture**: `docs/templates/annotated/deployment-architecture.md`
- **Behavioral specs (what to test)**: `docs/architecture/BEHAVIORAL_SPECS.md`