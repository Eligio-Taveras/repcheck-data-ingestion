package repcheck.ingestion.votes.app

import cats.effect.{ExitCode, IO, IOApp}

object VotesPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    VotesPipeline.run[IO](args)

}
