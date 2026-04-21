import org.typelevel.scalacoptions.ScalacOption
import sbt.Keys.libraryDependencies
import sbt.Def
import Dependencies.*
import com.repcheck.sbt.ExceptionUniquenessPlugin.autoImport.exceptionUniquenessRootPackages

val isScala212: Def.Initialize[Boolean] = Def.setting {
  VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("2.12.x"))
}

ThisBuild / dynverSonatypeSnapshots := true

// Cap concurrent test tasks across subprojects. Each DockerRequired-having subproject spins
// up its own AlloyDB Omni + Pub/Sub emulator (per-classloader singletons), so 4 concurrent
// test tasks means up to ~10 GB of containers. On a 31 GB dev box that's comfortable.
ThisBuild / concurrentRestrictions += Tags.limit(Tags.Test, 4)

// Common settings for all sub-projects
lazy val commonSettings = Seq(
  organization := "com.repcheck",
  scalaVersion := "3.7.3",
  publishTo := Some(
    "GitHub Packages" at s"https://maven.pkg.github.com/Eligio-Taveras/repcheck-data-ingestion"
  ),
  publishMavenStyle := true,
  credentials ++= {
    val envCreds = for {
      user  <- sys.env.get("GITHUB_ACTOR")
      token <- sys.env.get("GITHUB_TOKEN")
    } yield Credentials("GitHub Package Registry", "maven.pkg.github.com", user, token)

    val fileCreds = {
      val f = Path.userHome / ".sbt" / ".github-packages-credentials"
      if (f.exists) Some(Credentials(f)) else None
    }

    envCreds.orElse(fileCreds).toSeq
  },
  resolvers ++= Seq(
    "GitHub Packages - shared-models" at "https://maven.pkg.github.com/Eligio-Taveras/repcheck-shared-models",
    "GitHub Packages - pipeline-models" at "https://maven.pkg.github.com/Eligio-Taveras/repcheck-pipeline-models",
    "GitHub Packages - ingestion-common" at "https://maven.pkg.github.com/Eligio-Taveras/repcheck-ingestion-common",
    "GitHub Packages - db-migrations" at "https://maven.pkg.github.com/Eligio-Taveras/repcheck-db-migrations",
  ),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.18" % Test
  ),
  // Shared RepCheck dependencies consumed by all sub-projects
  libraryDependencies ++= Seq(
    "com.repcheck" %% "repcheck-pipeline-models"  % "0.1.17",
    "com.repcheck" %% "repcheck-ingestion-common" % "0.1.19",
    "com.repcheck" %% "repcheck-db-migrations-runner" % "0.1.18" % Test,
    "com.repcheck" %% "repchecksharedmodels"       % "0.1.24",
  ),
  semanticdbEnabled := true,
  tpolecatScalacOptions ++= ScalaCConfig.scalaCOptions,
  tpolecatScalacOptions ++= {
    if (isScala212.value) ScalaCConfig.scalaCOption2_12
    else Set.empty[ScalacOption]
  },
  // Circe semi-auto derivation for large case classes
  scalacOptions += "-Xmax-inlines:64",

  // WartRemover — enforces FP discipline at compile time
  wartremoverErrors ++= Seq(
    Wart.AsInstanceOf,          // No unsafe casts
    Wart.EitherProjectionPartial, // No .get on Either projections
    Wart.IsInstanceOf,          // No runtime type checks — use pattern matching
    Wart.MutableDataStructures, // No mutable collections
    Wart.Null,                  // No null — use Option
    Wart.OptionPartial,         // No Option.get — use fold/map/getOrElse
    Wart.Return,                // No return statements
    Wart.StringPlusAny,         // No string + any — use interpolation
    Wart.IterableOps,           // No .head/.tail on collections — use headOption
    Wart.TryPartial,            // No Try.get — use fold/recover
    Wart.Var                    // No mutable vars
  ),
  wartremoverWarnings ++= Seq(
    Wart.Throw                  // Warn on bare throw — prefer F.raiseError
  ),

  exceptionUniquenessRootPackages := Seq("com.repcheck", "repcheck"),

  // Suppress Scala 3.4-migration infix warnings for ScalaTest matchers in test sources
  Test / scalacOptions += "-Wconf:msg=is not declared infix:s",

  // Exclude Docker-backed and E2E tests from default `sbt test`. Run them explicitly via
  // `dockerTest` or `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`.
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "DockerRequired"),
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "com.repcheck.tags.E2ETest"),
)

// Pipeline-specific settings (IOApp projects get test config override)
lazy val pipelineSettings = commonSettings ++ Seq(
  Test / javaOptions += "-Dconfig.resource=application-test.conf",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "versions", _, _*)         => MergeStrategy.first
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
    case PathList("META-INF", "MANIFEST.MF")             => MergeStrategy.discard
    case PathList("META-INF", "services", _*)            => MergeStrategy.concat
    case PathList("module-info.class")                   => MergeStrategy.discard
    case x if x.endsWith(".proto")                       => MergeStrategy.first
    case x if x.endsWith(".properties")                  => MergeStrategy.first
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
)

lazy val root = (project in file("."))
  .aggregate(
    billsCommon,
    membersCommon,
    billMetadataPipeline,
    billTextAvailabilityChecker,
    billTextPipeline,
    memberProfilePipeline,
    lisMappingRefresher,
    docGenerator,
  )
  .settings(
    commonSettings,
    name := "repcheck-data-ingestion",
    publish / skip := true,
  )

lazy val billsCommon = (project in file("bills-common"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .settings(commonSettings)
  .settings(
    name := "bills-common",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig ++ fs2
      ++ catsEffect ++ doobie ++ pubSub ++ logging ++ testDeps,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    // Intra-subproject parallel execution causes FK violations because specs truncate shared
    // tables. Cross-subproject parallelism (via Tags.limit(Tags.Test, 4)) is safe because each
    // subproject gets its own AlloyDB container.
    Test / parallelExecution := false,
  )

lazy val membersCommon = (project in file("members-common"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .settings(commonSettings)
  .settings(
    name := "members-common",
    libraryDependencies ++= doobie ++ catsEffect ++ diff ++ logging ++ testDeps,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    Test / parallelExecution := false,
  )

lazy val billMetadataPipeline = (project in file("bill-metadata-pipeline"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .dependsOn(billsCommon % "compile->compile;test->test", membersCommon)
  .settings(pipelineSettings)
  .settings(
    name := "bill-metadata-pipeline",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
      ++ catsEffect ++ doobie ++ diff ++ logging ++ testDeps,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    coverageExcludedFiles := ".*BillMetadataPipeline;.*BillMetadataPipelineApp",
    assembly / mainClass := Some("repcheck.ingestion.bills.metadata.app.BillMetadataPipelineApp"),
    assembly / assemblyJarName := "bill-metadata-pipeline.jar",
  )

lazy val billTextAvailabilityChecker = (project in file("bill-text-availability-checker"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .dependsOn(billsCommon % "compile->compile;test->test")
  .settings(pipelineSettings)
  .settings(
    name := "bill-text-availability-checker",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
      ++ catsEffect ++ doobie ++ pubSub ++ fs2 ++ logging ++ testDeps,
    coverageExcludedFiles := ".*BillTextCheckerApp",
    assembly / mainClass := Some("repcheck.ingestion.bills.textcheck.app.BillTextCheckerApp"),
    assembly / assemblyJarName := "bill-text-availability-checker.jar",
  )

lazy val memberProfilePipeline = (project in file("member-profile-pipeline"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .dependsOn(membersCommon % "compile->compile;test->test")
  .dependsOn(lisMappingRefresher % "test->compile")
  .settings(pipelineSettings)
  .settings(
    name := "member-profile-pipeline",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
      ++ catsEffect ++ doobie ++ diff ++ pubSub ++ fs2 ++ logging ++ testDeps,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    coverageExcludedFiles := ".*MemberProfilePipelineApp",
    // Intra-subproject parallel execution causes FK violations because PlaceholderFillIntegrationSpec
    // and SenatorLifecycleIntegrationSpec share SharedDockerPostgres's singleton container and each
    // suite's afterEach truncates members / member_lis_mapping — which clobbers the other suite's
    // in-flight inserts. Matches the convention used by lisMappingRefresher.
    Test / parallelExecution := false,
    assembly / mainClass := Some("repcheck.ingestion.members.profile.app.MemberProfilePipelineApp"),
    assembly / assemblyJarName := "member-profile-pipeline.jar",
  )

lazy val lisMappingRefresher = (project in file("lis-mapping-refresher"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .dependsOn(membersCommon % "compile->compile;test->test")
  .settings(pipelineSettings)
  .settings(
    name := "lis-mapping-refresher",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
      ++ catsEffect ++ doobie ++ xml ++ pubSub ++ fs2 ++ logging ++ testDeps,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    coverageExcludedFiles := ".*LisMappingRefresherApp",
    // Intra-subproject parallel execution causes FK violations because DoobieLisMember*Spec and
    // DoobieLisMapping*Spec share SharedDockerPostgres's singleton container and each suite's
    // afterEach truncates lis_members / member_lis_mapping — which clobbers the other suite's
    // in-flight inserts. Matches the convention used by billsCommon and membersCommon.
    Test / parallelExecution := false,
    assembly / mainClass := Some("repcheck.members.lismapping.app.LisMappingRefresherApp"),
    assembly / assemblyJarName := "lis-mapping-refresher.jar",
  )

lazy val billTextPipeline = (project in file("bill-text-pipeline"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .dependsOn(billsCommon % "compile->compile;test->test")
  .dependsOn(billTextAvailabilityChecker % "test->compile")
  .dependsOn(billMetadataPipeline % "test->compile")
  .settings(pipelineSettings)
  .settings(
    name := "bill-text-pipeline",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig
      ++ catsEffect ++ doobie ++ pubSub ++ fs2 ++ xml ++ htmlParsing ++ logging ++ testDeps,
    coverageExcludedFiles := ".*BillTextPipelineApp",
    // WireMock-based tests share a dynamic port; sequential prevents port contention.
    // Cross-subproject parallelism (via Tags.limit(Tags.Test, 4)) gives us the speedup win.
    Test / parallelExecution := false,
    assembly / mainClass := Some("repcheck.ingestion.bills.text.app.BillTextPipelineApp"),
    assembly / assemblyJarName := "bill-text-pipeline.jar",
  )

// `dockerTest` runs only the DB-backed integration tests against a local AlloyDB Omni
// container. The default `Test / testOptions` exclude DockerRequired tests, so this alias
// overrides those options for the duration of the run and then restores them.
addCommandAlias(
  "dockerTest",
  "; set billsCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; billsCommon / test" +
    "; set billsCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"))" +
    "; set membersCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; membersCommon / test" +
    "; set membersCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"))" +
    "; set billTextPipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; billTextPipeline / test" +
    "; set billTextPipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))" +
    "; set billTextAvailabilityChecker / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; billTextAvailabilityChecker / test" +
    "; set billTextAvailabilityChecker / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))" +
    "; set memberProfilePipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; memberProfilePipeline / test" +
    "; set memberProfilePipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))" +
    "; set lisMappingRefresher / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; lisMappingRefresher / test" +
    "; set lisMappingRefresher / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))",
)

// `dockerTestParallel` — experimental. Flips every DockerRequired-capable subproject's test
// filter to include DockerRequired, then runs all subproject `test` tasks CONCURRENTLY via
// sbt's `all` command. Risks we're trying to surface: (a) shared DockerPostgres singleton
// contention across subprojects, (b) table truncation races, (c) Pub/Sub emulator subscription
// cross-talk, (d) JVM resource pressure from many concurrent parallel tests.
addCommandAlias(
  "dockerTestParallel",
  "; set billsCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; set membersCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; set billTextPipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; set billTextAvailabilityChecker / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; set memberProfilePipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; set lisMappingRefresher / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; all billsCommon/test membersCommon/test billTextPipeline/test billTextAvailabilityChecker/test memberProfilePipeline/test lisMappingRefresher/test" +
    "; set billsCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"))" +
    "; set membersCommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"))" +
    "; set billTextPipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))" +
    "; set billTextAvailabilityChecker / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))" +
    "; set memberProfilePipeline / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))" +
    "; set lisMappingRefresher / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"), Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"com.repcheck.tags.E2ETest\"))",
)

lazy val docGenerator = (project in file("doc-generator"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.anthropic" % "anthropic-java" % "2.18.0",
      "org.typelevel" %% "cats-effect" % "3.7.0",
      "ch.qos.logback" % "logback-classic" % "1.5.32"
    ),
    // Exclude WartRemover for this utility project — uses Java SDK patterns
    wartremoverErrors := Seq.empty,
    wartremoverWarnings := Seq.empty,
    // Exclude from coverage — utility project with no unit tests
    coverageEnabled := false
  )
