package repcheck.ingestion.bills.metadata.app

import cats.effect.{ExitCode, IO, IOApp}

object BillMetadataPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    BillMetadataPipeline.run[IO](args)

}
