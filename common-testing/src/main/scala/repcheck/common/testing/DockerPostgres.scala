package repcheck.common.testing

import cats.effect.{IO, Resource}

import repcheck.db.migrations.MigrationRunner

import com.repcheck.utils.testing.{PostgresContainerConfig, SharedPostgres}

/**
 * RepCheck wiring over the shared `repcheck-utils-testing-db` fixtures: the AlloyDB Omni image with the full Liquibase
 * changelog applied via the schema hook. The container mechanics (docker resolution, readiness, retry) live in
 * `com.repcheck.utils.testing`.
 */
object RepCheckPostgresConfig {

  val value: PostgresContainerConfig = PostgresContainerConfig(
    image = "google/alloydbomni:16.8.0",
    dbName = "repcheck_test",
    containerNamePrefix = "repcheck-test",
    initSchema = MigrationRunner.migrate,
  )

}

object DockerPostgres {

  val resource: Resource[IO, PostgresContainerInfo] =
    new com.repcheck.utils.testing.DockerPostgres(RepCheckPostgresConfig.value).resource

}

object SharedDockerPostgres extends SharedPostgres(RepCheckPostgresConfig.value)

type PostgresContainerInfo = com.repcheck.utils.testing.PostgresContainerInfo

val PostgresContainerInfo: com.repcheck.utils.testing.PostgresContainerInfo.type =
  com.repcheck.utils.testing.PostgresContainerInfo

val DockerRequired: org.scalatest.Tag = com.repcheck.utils.tags.DockerRequired

val E2ETest: org.scalatest.Tag = com.repcheck.utils.tags.E2ETest
