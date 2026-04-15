package repcheck.ingestion.bills.common.testing

import java.net.Socket

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.Try

final case class PubSubEmulatorInfo(host: String, port: Int) {
  def endpoint: String = s"$host:$port"
}

object DockerPubSub {

  private val image: String         = "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"
  private val internalPort: Int     = 8085
  private val maxReadyAttempts: Int = 60
  private val readyDelayMs: Long    = 1000L

  final private case class ContainerHandle(name: String, info: PubSubEmulatorInfo)

  def start(): (PubSubEmulatorInfo, () => Unit) = {
    val containerName = s"repcheck-pubsub-test-${java.util.UUID.randomUUID().toString.take(8)}"
    val port          = startContainer(containerName)
    waitForReady(port)
    val info    = PubSubEmulatorInfo("localhost", port)
    val cleanup = () => { val _ = Seq("docker", "rm", "-f", containerName).!; () }
    (info, cleanup)
  }

  private def startContainer(containerName: String): Int = {
    val exitCode = Seq(
      "docker",
      "run",
      "-d",
      "--name",
      containerName,
      "-p",
      s"0:$internalPort",
      image,
      "gcloud",
      "beta",
      "emulators",
      "pubsub",
      "start",
      s"--host-port=0.0.0.0:$internalPort",
    ).!

    if (exitCode != 0) {
      sys.error("Failed to start Pub/Sub emulator container. Is Docker running?")
    }

    val portOutput = Seq("docker", "port", containerName, internalPort.toString).!!.trim
    portOutput
      .split(':')
      .lastOption
      .getOrElse(sys.error(s"Unexpected docker port output: $portOutput"))
      .toInt
  }

  @tailrec
  private def waitForReady(port: Int, remaining: Int = maxReadyAttempts): Unit = {
    if (remaining <= 0) {
      sys.error(s"Pub/Sub emulator did not become ready after $maxReadyAttempts attempts")
    }

    val ready = Try {
      val socket = new Socket("localhost", port)
      socket.close()
    }.isSuccess

    if (!ready) {
      Thread.sleep(readyDelayMs)
      waitForReady(port, remaining - 1)
    }
  }

}

object SharedDockerPubSub {

  private lazy val handle: (PubSubEmulatorInfo, () => Unit) = {
    val (info, cleanup) = DockerPubSub.start()
    val _ = sys.addShutdownHook {
      cleanup()
    }
    (info, cleanup)
  }

  def info: PubSubEmulatorInfo = handle._1
}
