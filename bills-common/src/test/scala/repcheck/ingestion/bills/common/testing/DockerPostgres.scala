package repcheck.ingestion.bills.common.testing

/**
 * Backwards-compat re-exports of the (now-shared) Docker-backed Postgres test fixtures. The implementation lives in the
 * `common-testing` sub-project at `repcheck.common.testing` — a single source of truth that both `bills-common` and
 * `members-common` (and any future test suite that needs a real Postgres) consume. This file exists so the 32+ existing
 * test specs that `import repcheck.ingestion.bills.common.testing.{DockerRequired, ...}` keep compiling without churn.
 *
 * New test code should import directly from `repcheck.common.testing` — these re-exports may be removed in a future
 * pass if the existing imports get migrated.
 */
object DockerPostgres {
  export repcheck.common.testing.DockerPostgres.resource
}

object SharedDockerPostgres {
  def info: repcheck.common.testing.PostgresContainerInfo = repcheck.common.testing.SharedDockerPostgres.info
}

type PostgresContainerInfo = repcheck.common.testing.PostgresContainerInfo

val PostgresContainerInfo: repcheck.common.testing.PostgresContainerInfo.type =
  repcheck.common.testing.PostgresContainerInfo

object DockerRequired extends org.scalatest.Tag("DockerRequired")

object E2ETest extends org.scalatest.Tag("com.repcheck.tags.E2ETest")
