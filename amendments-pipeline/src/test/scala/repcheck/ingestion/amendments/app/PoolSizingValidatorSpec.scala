package repcheck.ingestion.amendments.app

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.amendments.config.AmendmentsConfig
import repcheck.ingestion.common.db.DatabaseConfig

class PoolSizingValidatorSpec extends AnyFlatSpec with Matchers {

  private def db(maxConnections: Int): DatabaseConfig =
    DatabaseConfig(
      host = "h",
      port = 0,
      database = "db",
      username = "u",
      password = "p",
      maxConnections = maxConnections,
    )

  "validate" should "return None when maxConnections == required" in {
    val cfg = AmendmentsConfig(parallelism = 4, maxRecursionDepth = 10)
    PoolSizingValidator.validate(db(45), cfg) shouldBe None
  }

  it should "return None when maxConnections > required" in {
    val cfg = AmendmentsConfig(parallelism = 4, maxRecursionDepth = 10)
    PoolSizingValidator.validate(db(60), cfg) shouldBe None
  }

  it should "return Some(message) when maxConnections < required" in {
    val cfg    = AmendmentsConfig(parallelism = 4, maxRecursionDepth = 10)
    val result = PoolSizingValidator.validate(db(10), cfg)
    val _      = result.isDefined shouldBe true
    result.foreach { msg =>
      val _ = msg should include("10")
      msg should include("45")
    }
  }

  it should "compute the required value as parallelism × maxRecursionDepth + 5" in {
    val cfg = AmendmentsConfig(parallelism = 2, maxRecursionDepth = 3)
    // 2 × 3 + 5 = 11, so a pool of 11 must validate; 10 must not.
    val _ = PoolSizingValidator.validate(db(maxConnections = 11), cfg) shouldBe None
    PoolSizingValidator.validate(db(maxConnections = 10), cfg).isDefined shouldBe true
  }

}
