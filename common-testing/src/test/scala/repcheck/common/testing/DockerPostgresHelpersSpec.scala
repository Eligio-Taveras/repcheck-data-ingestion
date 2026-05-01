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

}
