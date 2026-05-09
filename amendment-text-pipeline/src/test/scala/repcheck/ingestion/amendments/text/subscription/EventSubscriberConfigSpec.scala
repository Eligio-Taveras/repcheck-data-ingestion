package repcheck.ingestion.amendments.text.subscription

import scala.concurrent.duration.DurationInt

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventSubscriberConfigSpec extends AnyFlatSpec with Matchers {

  "EventSubscriberConfig" should "load all fields from a HOCON snippet" in {
    val raw = ConfigFactory.parseString("""
      project-id = "repcheck-test"
      subscription-id = "amendment-text-available-sub"
      max-messages = 50
      pull-timeout = 15s
    """)
    val config = ConfigSource.fromConfig(raw).loadOrThrow[EventSubscriberConfig]
    val _      = config.projectId shouldBe "repcheck-test"
    val _      = config.subscriptionId shouldBe "amendment-text-available-sub"
    val _      = config.maxMessages shouldBe 50
    config.pullTimeout shouldBe 15.seconds
  }

}
