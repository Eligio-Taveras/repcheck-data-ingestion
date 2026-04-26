package repcheck.ingestion.bills.summary.app

import cats.effect.{ExitCode, IO, IOApp}

object BillSummaryPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    BillSummaryPipeline.run[IO](args)

}
