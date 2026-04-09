<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/orchestrator.scala -->

# RepCheck Skeleton: Pipeline Orchestrator

**Purpose:** Cloud Scheduler-triggered Cloud Run Job. Reads Pub/Sub queue, checks Cloud Run capacity against message ResourceRequirements, launches Cloud Run Jobs when capacity available, re-enqueues when not, dead-letters after max retries with GCP Monitoring alert.

**Key Decisions:**
- Orchestrator is Cloud Run Job triggered by Cloud Scheduler
- Each PipelineEvent carries ResourceRequirements (maxCpu, maxMemory)
- Checks Cloud Run API for available capacity
- Capacity available → launch Cloud Run Job execution
- No capacity → re-enqueue with retryCount + 1
- retryCount >= maxRetries → move to dead-letter queue with GCP Monitoring alert

---

```scala
package repcheck.orchestrator

import cats.effect.{Async, IOApp, ExitCode, IO}
import cats.syntax.all.*
import java.util.UUID
import repcheck.pipeline.models.pubsub.*
import repcheck.pipeline.models.retry.*

// Cloud Run Capacity Checker

trait CapacityChecker[F[_]] {
  def hasCapacity(requirements: ResourceRequirements): F[Boolean]
}

object CapacityChecker {
  def make[F[_]: Async](
      projectId: String,
      region: String
  ): CapacityChecker[F] =
    new CapacityChecker[F] {
      def hasCapacity(requirements: ResourceRequirements): F[Boolean] =
        Async[F].blocking {
          // TODO: Query Cloud Run Admin API for current resource utilization
          // 1. List running job executions: GET https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{job}/executions
          // 2. Sum current CPU/memory usage across running executions
          // 3. Compare against Cloud Run service limits and ResourceRequirements (maxCpu, maxMemory)
          // 4. Return true if adding this job would stay within limits
          // Use com.google.cloud.run.v2.JobsClient or HTTP API via http4s
          ???
        }
    }
}

// Cloud Run Job Launcher

trait JobLauncher[F[_]] {
  def launch(jobName: String, event: io.circe.Json, resources: ResourceRequirements): F[String]
}

object JobLauncher {
  def make[F[_]: Async](
      projectId: String,
      region: String
  ): JobLauncher[F] =
    new JobLauncher[F] {
      def launch(jobName: String, event: io.circe.Json, resources: ResourceRequirements): F[String] =
        Async[F].blocking {
          // TODO: Use Cloud Run Admin API to create job execution
          // val client = com.google.cloud.run.v2.JobsClient.create()
          // val request = RunJobRequest.newBuilder()
          //   .setName(s"projects/$projectId/locations/$region/jobs/$jobName")
          //   .setOverrides(RunJobRequest.Overrides.newBuilder()
          //     .addContainerOverrides(
          //       RunJobRequest.Overrides.ContainerOverride.newBuilder()
          //         .addEnv(EnvVar.newBuilder()
          //           .setName("PIPELINE_EVENT")
          //           .setValue(event.noSpaces)
          //           .build())
          //         .build())
          //     .build())
          //   .build()
          // val operation = client.runJobAsync(request)
          // operation.getName  // execution ID
          ???
        }
    }
}

// Event Router

final case class EventRoute(eventType: String, targetJobName: String)

object EventRouter {
  val routes: Map[String, String] = Map(
    "bill.text.available"  -> "repcheck-llm-analysis",
    "vote.recorded"        -> "repcheck-scoring-engine",
    "analysis.completed"   -> "repcheck-scoring-engine",
    "user.profile.updated" -> "repcheck-scoring-engine",
    "snapshot.requested"   -> "repcheck-snapshot-service"
  )
  def targetJob(eventType: String): Option[String] = routes.get(eventType)
}

// Orchestrator Logic

trait Orchestrator[F[_]] {
  def processQueue(): F[Int]
}

object Orchestrator {
  final case class OrchestratorConfig(
      projectId: String,
      region: String,
      mainSubscriptionId: String,
      deadLetterTopicId: String,
      maxMessagesPerRun: Int = 50
  )

  def make[F[_]: Async](
      config: OrchestratorConfig,
      subscriber: PubSubSubscriber[F],
      publisher: PubSubPublisher[F],
      deadLetterPublisher: PubSubPublisher[F],
      capacityChecker: CapacityChecker[F],
      jobLauncher: JobLauncher[F]
  ): Orchestrator[F] =
    new Orchestrator[F] {
      def processQueue(): F[Int] =
        subscriber.pullAndProcess[io.circe.Json](
          config.maxMessagesPerRun,
          handleEvent
        )

      private def handleEvent(event: PipelineEvent[io.circe.Json]): F[Unit] =
        EventRouter.targetJob(event.eventType) match {
          case None =>
            // TODO: Log WARN: s"Unknown event type: ${event.eventType}, dead-lettering"
            deadLetterPublisher.publish(event).void

          case Some(jobName) =>
            for {
              available <- capacityChecker.hasCapacity(event.resources)
              _ <-
                if (available) {
                  // TODO: Log INFO: s"Launching $jobName for event ${event.eventId}"
                  jobLauncher.launch(jobName, event.payload, event.resources).void
                } else if (event.retryCount >= event.maxRetries) {
                  // TODO: Log ERROR: s"Dead-lettering event ${event.eventId} after ${event.retryCount} retries"
                  deadLetterPublisher.publish(event).void
                } else {
                  val requeued = event.copy(retryCount = event.retryCount + 1)
                  // TODO: Log INFO: s"Re-enqueuing event ${event.eventId} (retry ${requeued.retryCount}/${event.maxRetries})"
                  publisher.publish(requeued).void
                }
            } yield ()
        }
    }
}

// Orchestrator App Entry Point

// TODO: Uncomment and implement when ready
//
// object OrchestratorApp extends IOApp {
//   override def run(args: List[String]): IO[ExitCode] =
//     for {
//       config <- ConfigLoader.load[OrchestratorConfig]("orchestrator")
//       _ <- (
//         PubSubSubscriber.make[IO](...),
//         PubSubPublisher.make[IO](...),  // main topic (re-enqueue)
//         PubSubPublisher.make[IO](...),  // dead-letter topic
//       ).tupled.use { case (subscriber, publisher, deadLetterPublisher) =>
//         val capacityChecker = CapacityChecker.make[IO](config.projectId, config.region)
//         val jobLauncher = JobLauncher.make[IO](config.projectId, config.region)
//         val orchestrator = Orchestrator.make[IO](
//           config, subscriber, publisher, deadLetterPublisher,
//           capacityChecker, jobLauncher
//         )
//         orchestrator.processQueue().flatMap { count =>
//           IO.println(s"Processed $count messages")
//         }
//       }
//     } yield ExitCode.Success
// }
```