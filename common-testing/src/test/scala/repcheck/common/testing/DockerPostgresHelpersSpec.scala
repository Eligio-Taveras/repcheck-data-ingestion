package repcheck.common.testing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for the pure helpers extracted from DockerPostgres so the IO-bound shell-out paths can stay testably
 * isolated from the platform-resolution + parsing logic. The IO paths themselves (acquire / release / startContainer)
 * are exercised by every DockerRequired-tagged spec across the repo and don't have unit-level coverage.
 */
class DockerPostgresHelpersSpec extends AnyFlatSpec with Matchers {

  "resolveDockerBin" should "honor DOCKER_BIN env override regardless of OS" in {
    val _ = DockerPostgres.resolveDockerBin(Some("Linux"), Some("/custom/docker")) shouldBe "/custom/docker"
    DockerPostgres.resolveDockerBin(
      Some("Windows 10"),
      Some("C:\\custom\\docker.exe"),
    ) shouldBe "C:\\custom\\docker.exe"
  }

  it should "use the Windows Docker Desktop absolute path when os.name contains 'windows'" in {
    val _ = DockerPostgres.resolveDockerBin(Some("Windows 11"), None) should include("docker.exe")
    DockerPostgres.resolveDockerBin(Some("Windows 10"), None) should include("Program Files")
  }

  it should "fall back to bare 'docker' on Linux / macOS / unknown OS" in {
    val _ = DockerPostgres.resolveDockerBin(Some("Linux"), None) shouldBe "docker"
    val _ = DockerPostgres.resolveDockerBin(Some("Mac OS X"), None) shouldBe "docker"
    DockerPostgres.resolveDockerBin(None, None) shouldBe "docker"
  }

  "parseHostPort" should "extract the trailing port from `host:port` output" in {
    val _ = DockerPostgres.parseHostPort("0.0.0.0:54321\n") shouldBe 54321
    DockerPostgres.parseHostPort("[::]:9999") shouldBe 9999
  }

  it should "raise on output that has no colon (treats whole string as int)" in {
    // `lastOption.getOrElse` returns the only segment when there's no colon — `.toInt` then fails on non-numeric.
    a[NumberFormatException] should be thrownBy DockerPostgres.parseHostPort("not-a-port")
  }

  it should "raise on empty output via the explicit getOrElse error" in {
    // `split(':')` on "" returns Array("") → lastOption = Some("") → .toInt throws. Documents the failure mode
    // (operators see a NumberFormatException, which we live with — `docker port` shouldn't return empty).
    a[NumberFormatException] should be thrownBy DockerPostgres.parseHostPort("")
  }

  // connectWithRetry is normally driven by `acquire` after waitForReady gates on pg_isready, so its retry +
  // exhaustion branches never fire in the happy path. Drive them directly here with a port nothing's listening on
  // — JDBC fails immediately with no docker required, making this a unit-tier check for the final `Failure(ex)`
  // branch (line 155-156) that codecov flagged as uncovered on PR #107.
  "connectWithRetry" should "raise after exhausting attempts (remaining=1, immediate failure)" in {
    val ex = the[RuntimeException] thrownBy DockerPostgres.connectWithRetry(port = 1, remaining = 1)
    val _  = ex.getMessage should include("Failed to connect to PostgreSQL")
    ex.getMessage should include("after")
  }

  it should "exercise the retry sleep path (remaining=2 → first failure → recurse → final failure)" in {
    // remaining=2 takes the `Failure(_) if remaining > 1` branch on first iter (line 152-154 — sleep + recurse),
    // then the `Failure(ex)` branch on the second iter (line 155-156). Two attempts × 1s sleep + connect-refused
    // turnaround keeps this under a couple seconds; not a perf-sensitive suite.
    a[RuntimeException] should be thrownBy DockerPostgres.connectWithRetry(port = 1, remaining = 2)
  }

  // requireContainerStartSucceeded + failOnReadinessExhaustion are extracted helpers (see DockerPostgres.scala
  // header comment) — kept as testable seams so codecov sees the failure-throw branches without needing to
  // induce a real container failure mid-startup.

  "requireContainerStartSucceeded" should "be a no-op on exit code 0" in {
    noException should be thrownBy DockerPostgres.requireContainerStartSucceeded(0)
  }

  it should "raise on a non-zero exit code" in {
    val ex = the[RuntimeException] thrownBy DockerPostgres.requireContainerStartSucceeded(1)
    ex.getMessage should include("Failed to start Docker container")
  }

  "failOnReadinessExhaustion" should "invoke the cleanup hook and raise" in {
    val cleanupRan = new java.util.concurrent.atomic.AtomicBoolean(false)
    val ex = the[RuntimeException] thrownBy DockerPostgres.failOnReadinessExhaustion(
      cleanup = () => { val _ = cleanupRan.set(true); () },
      maxAttempts = 120,
    )
    val _ = cleanupRan.get() shouldBe true
    ex.getMessage should include("did not become ready after 120 attempts")
  }

}
