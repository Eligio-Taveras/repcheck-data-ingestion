package repcheck.ingestion.bills.metadata.app

import cats.effect.{ExitCode, IO, IOApp}

object BillMetadataPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    IO.println(
      s"[bootstrap] BillMetadataPipelineApp.run starting (args=${args.mkString("[", ",", "]")}, " +
        s"jvmTime=${java.time.Instant.now().toString})"
    ) *> BillMetadataPipeline.run[IO](args)

}
