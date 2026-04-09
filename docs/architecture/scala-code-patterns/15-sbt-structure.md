> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 16. SBT Project Structure

**Pattern**: Repos that have both publishable types and application code use a `models/` + `app/` sub-project structure. Pure library repos are single-project.

### Multi-Project Repo (e.g., `repcheck-data-ingestion`)

```scala
// build.sbt
lazy val commonSettings = Seq(
  organization := "com.repcheck",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "3.4.1",
  semanticdbEnabled := true
)

lazy val root = (project in file("."))
  .aggregate(models, ingestionCommon, billsPipeline, votesPipeline, membersPipeline, amendmentsPipeline)

// Published as "repcheck-data-ingestion-models"
lazy val models = (project in file("models"))
  .settings(
    commonSettings,
    name := "repcheck-data-ingestion-models",
    libraryDependencies ++= circe ++ Seq(
      "com.repcheck" %% "repcheck-shared-models" % Versions.sharedModels
    )
  )

lazy val ingestionCommon = (project in file("ingestion-common"))
  .settings(
    commonSettings,
    libraryDependencies ++= http4sEmber ++ fs2 ++ Seq(
      "com.repcheck" %% "repcheck-pipeline-models" % Versions.pipelineModels
    )
  )
  .dependsOn(models)

lazy val billsPipeline = (project in file("pipelines/bills"))
  .settings(
    commonSettings,
    libraryDependencies ++= pureConfig ++ firebase
  )
  .dependsOn(ingestionCommon)

lazy val votesPipeline = (project in file("pipelines/votes"))
  .settings(commonSettings, libraryDependencies ++= pureConfig ++ alloydb)
  .dependsOn(ingestionCommon)

lazy val membersPipeline = (project in file("pipelines/members"))
  .settings(commonSettings, libraryDependencies ++= pureConfig ++ alloydb)
  .dependsOn(ingestionCommon)

lazy val amendmentsPipeline = (project in file("pipelines/amendments"))
  .settings(commonSettings, libraryDependencies ++= pureConfig ++ alloydb)
  .dependsOn(ingestionCommon)
```

### Single-Project Repo (e.g., `repcheck-shared-models`)

```scala
// build.sbt
lazy val root = (project in file("."))
  .settings(
    organization := "com.repcheck",
    name := "repcheck-shared-models",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "3.4.1",
    libraryDependencies ++= circe
  )
```

### Dependencies File

```scala
// project/Dependencies.scala
import sbt.*

object Dependencies {
  object Versions {
    val http4s      = "0.23.26"
    val circe       = "0.14.6"
    val pureConfig  = "0.17.6"
    val fs2         = "3.10.2"
    val doobie      = "1.0.0-RC4"
    val scalaTest   = "3.2.18"
  }

  val circe = Seq(
    "io.circe" %% "circe-core"    % Versions.circe,
    "io.circe" %% "circe-generic" % Versions.circe,
    "io.circe" %% "circe-parser"  % Versions.circe
  )

  val http4sEmber = Seq(
    "org.http4s" %% "http4s-ember-client" % Versions.http4s,
    "org.http4s" %% "http4s-ember-server" % Versions.http4s,
    "org.http4s" %% "http4s-circe"        % Versions.http4s,
    "org.http4s" %% "http4s-dsl"          % Versions.http4s
  )

  val fs2 = Seq(
    "co.fs2" %% "fs2-core"             % Versions.fs2,
    "co.fs2" %% "fs2-io"               % Versions.fs2,
    "co.fs2" %% "fs2-reactive-streams" % Versions.fs2
  )

  val alloydb = Seq(
    "org.tpolecat" %% "doobie-core"           % Versions.doobie,
    "org.tpolecat" %% "doobie-hikari"          % Versions.doobie,
    "org.tpolecat" %% "doobie-postgres"        % Versions.doobie,
    "org.postgresql" % "postgresql"            % "42.7.3"
  )

  val pureConfig = Seq(
    "com.github.pureconfig" %% "pureconfig-core"           % Versions.pureConfig,
    "com.github.pureconfig" %% "pureconfig-generic-scala3" % Versions.pureConfig
  )

  val doobie = Seq(
    "org.tpolecat" %% "doobie-core"     % Versions.doobie,
    "org.tpolecat" %% "doobie-hikari"   % Versions.doobie,
    "org.tpolecat" %% "doobie-postgres" % Versions.doobie
  )

  val testing = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest % Test
  )
}
```

### GitHub Packages Publishing

```scala
// In build.sbt for publishable projects
publishTo := Some(
  "GitHub Packages" at s"https://maven.pkg.github.com/Eligio-Taveras/${name.value}"
)
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_ACTOR", ""),
  sys.env.getOrElse("GITHUB_TOKEN", "")
)
```

### Rules
- Multi-project repos: `models/` sub-project is published; pipeline/app sub-projects are not
- Single-project repos (pure libraries): the root project is published
- `project/Dependencies.scala` centralizes all library versions
- `project/Versions.scala` centralizes repcheck artifact versions
- Organization is `com.repcheck` for all artifacts
- GitHub Packages for artifact hosting
