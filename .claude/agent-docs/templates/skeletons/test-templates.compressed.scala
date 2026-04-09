<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/test-templates.scala -->

```markdown
# RepCheck Skeleton: Test Patterns

**Repo:** Every application repo  
**Purpose:** Four test scaffold patterns: unit tests, equivalence class negative testing, local integration tests, dev GCP contract tests.

**Key Decisions:**
- Unit tests: http4s test client with canned responses
- Negative testing: analyze each function line for failure inputs using equivalence classes, create test per scenario
- If production code lacks error handling → add one first with unique exception
- Every exception stack trace tells exactly where and why
- Correlation ID visible in all test assertions
- Integration (local): DockerPostgresSpec (custom Docker CLI, pgvector:pg16)
- Integration (WireMock): failure simulation only (timeouts, errors, malformed responses) with validated fixtures
- Integration (dev GCP): contract/connection validation against real dev project, namespaced per developer/CI
- All tests run on every PR commit, must pass locally with Docker

---

## Pattern A: Unit Test with http4s Test Client

Tests API client behavior using in-memory http4s client. No network calls — fully deterministic.

```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.http4s._
import org.http4s.client.Client
import org.http4s.dsl.io._
import io.circe.syntax._

class ApiClientUnitSpec extends AnyFlatSpec with Matchers {

  val mockClient: Client[IO] = Client.fromHttpApp(
    HttpRoutes.of[IO] {
      // Happy path — return valid bill data
      case GET -> Root / "v3" / "bill" =>
        Ok(validBillsResponse.asJson)

      // Rate limited — 429 response
      case GET -> Root / "v3" / "bill" :? RateLimitParam(true) =>
        TooManyRequests("Rate limit exceeded")

      // Server error — 500
      case GET -> Root / "v3" / "bill" :? ServerErrorParam(true) =>
        InternalServerError("Internal server error")

      // Malformed JSON response
      case GET -> Root / "v3" / "bill" :? MalformedParam(true) =>
        Ok("{not valid json")
    }.orNotFound
  )

  "ApiClient" should "parse valid response into DTOs" in {
    val api = LegislativeBillsApi[IO](mockClient, ...)
    val result = api.defaultCall().unsafeRunSync()
    result.bills should not be empty
    result.bills.head.billId should be("hr-1234")
  }

  it should "propagate rate limit errors as Transient" in {
    intercept[TransientFailure] {
      api.fetchWithRateLimit().unsafeRunSync()
    }
  }

  it should "handle malformed JSON with DecodeFailed" in {
    val error = intercept[DecodeFailed] {
      api.fetchMalformed().unsafeRunSync()
    }
    error.entityId should be("unknown")
    error.getMessage should include("decode")
  }
}
```

---

## Pattern B: Equivalence Class Negative Testing

For each function, analyze every line for potential failure scenarios. Group inputs into equivalence classes, test one representative per class.

**Process:**
1. Read function line by line
2. For each line, ask: "What input would make this fail?"
3. Group failure inputs into equivalence classes
4. Write one test per equivalence class
5. If production code lacks handler → add one first with unique exception

### Example: BillTypes

```scala
class BillTypesNegativeSpec extends AnyFlatSpec with Matchers {

  // Target: def fromString(s: String): Either[String, BillTypes]
  // Line analysis:
  // - Input: empty string → no match → Left("Unrecognized bill type: ")
  // - Input: "hr" (lowercase) → no match (case-sensitive)
  // - Input: "HR" → match → Right(BillTypes.HR)
  // - Input: "INVALID" → no match → Left("Unrecognized bill type: INVALID")
  // - Input: "HR " (trailing space) → no match
  //
  // Equivalence classes:
  // 1. Valid uppercase code → Right
  // 2. Valid code wrong case → Left
  // 3. Completely unknown code → Left
  // 4. Empty string → Left
  // 5. Code with whitespace → Left

  "BillTypes.fromString" should "parse valid uppercase bill type code" in {
    BillTypes.fromString("HR") should be(Right(BillTypes.HR))
    BillTypes.fromString("SJRES") should be(Right(BillTypes.SJRES))
  }

  it should "reject lowercase variant of valid code" in {
    BillTypes.fromString("hr") should be(Left("Unrecognized bill type: hr"))
  }

  it should "reject completely unknown code" in {
    BillTypes.fromString("INVALID") should be(Left("Unrecognized bill type: INVALID"))
  }

  it should "reject empty string" in {
    BillTypes.fromString("") should be(Left("Unrecognized bill type: "))
  }

  it should "reject code with trailing whitespace" in {
    BillTypes.fromString("HR ") should be(Left("Unrecognized bill type: HR "))
  }
}
```

### Example: RetryWrapper

```scala
class RetryWrapperNegativeSpec extends AnyFlatSpec with Matchers {

  // Target: RetryWrapper.withRetry
  // Equivalence classes:
  // 1. Operation succeeds on first try → no retry needed
  // 2. Operation fails, then succeeds within retries → should succeed
  // 3. Operation fails with Transient error, exhausts all retries → TransientFailure
  // 4. Operation fails with Systemic error on first try → SystemicFailure immediately
  // 5. Operation times out → treated as classifier says
  // 6. Backoff respects maxBackoff cap

  "RetryWrapper" should "succeed without retry on first success" in {
    pending
  }

  it should "succeed after transient failure then recovery" in {
    // Use counter to fail first N times, then succeed
    pending
  }

  it should "throw TransientFailure after exhausting retries" in {
    intercept[TransientFailure] { /* always fail with Transient */ }
  }

  it should "throw SystemicFailure immediately without retry" in {
    intercept[SystemicFailure] { /* fail with Systemic */ }
    // Verify only attempted once (no retries)
  }

  it should "respect timeout per operation" in {
    // Operation taking longer than config.timeout
    // Expect TimeoutException (classified by ErrorClassifier)
    pending
  }

  it should "cap backoff at maxBackoff" in {
    // After many retries, backoff should not exceed config.maxBackoff
    pending
  }
}
```

---

## Pattern C: Integration Test with DockerPostgresSpec

Local integration tests using DockerPostgresSpec from db-migrations. Requires Docker running locally. Runs in CI on every PR commit.

**Prerequisites:**
- `build.sbt`: `.dependsOn(dbMigrations % "test->test")`
- Docker must be running

**DockerPostgresSpec handles:**
- Starting AlloyDB Omni (google/alloydbomni:16.8.0) container with random port
- Applying all Liquibase migrations (full schema)
- Exposing `jdbcUrl` and `getConnection`
- Removing container in `afterAll`

**IMPORTANT:** Test data must have unique values for ALL unique constraints on target table, not just PK. Check migration SQL in db-migrations. Expose parameters for every constrained column in test data helpers.

```scala
import doobie.implicits._
import doobie.util.transactor.Transactor
import repcheck.db.migrations.DockerPostgresSpec

class MyRepositoryIntegrationSpec extends AnyFlatSpec
    with Matchers
    with DockerPostgresSpec {

  // Use fromDriverManager — manages connection lifecycle per transaction.
  // Do NOT use fromConnection (shares single connection, causes issues).
  private lazy val xa: Transactor[IO] =
    Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver",
      url = jdbcUrl,
      user = "test",
      password = "test",
      logHandler = None,
    )

  // Helper: expose ALL unique-constraint columns as parameters
  private def makeEntity(
    id: String = "default-id",
    naturalKey1: Int = 1,
    naturalKey2: String = "type",
    title: String = "Default",
  ): MyEntityDO = MyEntityDO(id, naturalKey1, naturalKey2, title, ...)

  // Verification helper: read back from DB to confirm persistence
  private def findTitle(id: String): IO[String] =
    sql"SELECT title FROM my_table WHERE id = $id".query[String].unique.transact(xa)

  "MyRepository.upsert" should "insert a new entity and return the id" in {
    val repo   = MyRepository.make[IO](xa)
    val entity = makeEntity(id = "test-1", naturalKey1 = 100)
    val result = repo.upsert(entity).unsafeRunSync()
    result shouldBe "test-1"
  }

  it should "persist the entity so it can be read back" in {
    val repo = MyRepository.make[IO](xa)
    repo.upsert(makeEntity(id = "test-2", naturalKey1 = 200, title = "Persisted")).unsafeRunSync()
    findTitle("test-2").unsafeRunSync() shouldBe "Persisted"
  }

  it should "update an existing entity on conflict" in {
    val repo = MyRepository.make[IO](xa)
    repo.upsert(makeEntity(id = "test-3", naturalKey1 = 300, title = "Original")).unsafeRunSync()
    repo.upsert(makeEntity(id = "test-3", naturalKey1 = 300, title = "Updated")).unsafeRunSync()
    findTitle("test-3").unsafeRunSync() shouldBe "Updated"
  }

  it should "not create duplicate rows on upsert" in {
    val repo        = MyRepository.make[IO](xa)
    val entity      = makeEntity(id = "test-4", naturalKey1 = 400)
    val beforeCount = sql"SELECT COUNT(*) FROM my_table".query[Int].unique.transact(xa).unsafeRunSync()
    repo.upsert(entity).unsafeRunSync()
    repo.upsert(entity).unsafeRunSync()
    val afterCount = sql"SELECT COUNT(*) FROM my_table".query[Int].unique.transact(xa).unsafeRunSync()
    (afterCount - beforeCount) shouldBe 1
  }
}
```

---

## Pattern C2: WireMock for Failure Simulation

WireMock tests simulate failure scenarios from external services: timeouts, error responses, malformed responses, rate limits.

**NOTE:** Response fixtures must be validated against known real responses. If you don't know what a server would return, ask the project lead first.

```scala
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration

class ExternalApiFailureSpec extends AnyFlatSpec with Matchers {

  val wireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort())

  override def beforeAll(): Unit = wireMock.start()
  override def afterAll(): Unit = wireMock.stop()

  "BillsApi" should "handle 500 server error gracefully" in {
    wireMock.stubFor(
      get(urlPathMatching("/v3/bill.*"))
        .willReturn(aResponse().withStatus(500).withBody("Internal Server Error"))
    )
    // Expect TransientFailure after retries exhausted
  }

  it should "handle timeout gracefully" in {
    wireMock.stubFor(
      get(urlPathMatching("/v3/bill.*"))
        .willReturn(aResponse().withFixedDelay(60000))
    )
    // Expect timeout → TransientFailure or SystemicFailure depending on classifier
  }

  it should "handle malformed JSON response" in {
    wireMock.stubFor(
      get(urlPathMatching("/v3/bill.*"))
        .willReturn(aResponse().withStatus(200).withBody("{invalid json"))
    )
    // Expect DecodeFailed
  }

  it should "handle rate limiting (429)" in {
    wireMock.stubFor(
      get(urlPathMatching("/v3/bill.*"))
        .willReturn(aResponse().withStatus(429).withBody("Rate limit exceeded"))
    )
    // Expect retries, then TransientFailure
  }
}
```

---

## Pattern D: Dev GCP Contract/Connection Tests

Integration tests against real GCP dev environment. Validates contracts (request/response shapes) and connectivity. Namespaced per developer/CI run to avoid collisions.

**NOT for failure simulation** (use WireMock for that). Verifies code actually connects and contract is correct.

```scala
class AlloyDbContractSpec extends AnyFlatSpec with Matchers {

  // Namespace: each test run uses unique schema prefix
  val testNamespace = s"test_${System.getenv("USER")}_${System.currentTimeMillis()}"

  // Clean up after tests
  override def afterAll(): Unit =
    // sql"DROP SCHEMA IF EXISTS $testNamespace CASCADE".update.run.transact(xa).unsafeRunSync()
    pending

  "AlloyDbRepository" should "write and read a bill row" in {
    val repo = AlloyDbBillRepository.make[IO](devTransactor, AlloyDbErrorClassifier)
    val billDO = createTestBill()

    val result = (for {
      _ <- repo.upsert(billDO)
      retrieved <- repo.findById(billDO.billId)
    } yield retrieved).unsafeRunSync()

    result should be(Some(billDO))
  }

  it should "return None for non-existent row" in {
    pending
  }

  it should "preserve correlationId in stored row" in {
    val correlationId = UUID.randomUUID()
    // Write with correlationId in metadata
    // Read back and verify correlationId matches
    pending
  }
}
```
```