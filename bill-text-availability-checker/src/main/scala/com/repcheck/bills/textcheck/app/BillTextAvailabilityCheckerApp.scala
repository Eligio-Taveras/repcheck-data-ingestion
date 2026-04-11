package com.repcheck.bills.textcheck.app

import cats.effect.{ExitCode, IO, IOApp}

object BillTextAvailabilityCheckerApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    BillTextCheckerPipeline.run[IO](args)

}
