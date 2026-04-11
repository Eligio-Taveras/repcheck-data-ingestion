package com.repcheck.bills.text.app

import cats.effect.{ExitCode, IO, IOApp}

object BillTextPipelineApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    BillTextPipelinePipeline.run[IO](args)

}
