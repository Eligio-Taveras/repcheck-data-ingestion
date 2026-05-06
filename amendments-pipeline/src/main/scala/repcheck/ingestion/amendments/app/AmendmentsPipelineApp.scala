package repcheck.ingestion.amendments.app

import cats.effect.{ExitCode, IO, IOApp}

/**
 * Entry point for the amendments-pipeline Cloud Run Job. Per CLAUDE.md's testability rule, the `IOApp` body is pure
 * wiring — it delegates to [[AmendmentsPipeline.run]] which in turn calls [[AmendmentsPipeline.runWithFactories]] with
 * real production factories. All testable orchestration logic lives in [[AmendmentsPipeline.runWithFactories]] so unit
 * tests inject stubs without standing up real GCP / Congress.gov / AlloyDB resources.
 */
object AmendmentsPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    IO.println(
      s"[bootstrap] AmendmentsPipelineApp.run starting (args=${args.mkString("[", ",", "]")}, " +
        s"jvmTime=${java.time.Instant.now().toString})"
    ) *> AmendmentsPipelineRun.run[IO](args)

}
