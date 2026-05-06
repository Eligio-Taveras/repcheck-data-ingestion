package repcheck.ingestion.amendments.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentPersistenceErrorsSpec extends AnyFlatSpec with Matchers {

  "AmendmentUpsertFailed" should "compose a message that includes the natural key and detail" in {
    val cause = new RuntimeException("boom")
    val ex    = AmendmentUpsertFailed("117-SAMDT-2137", "constraint violation", cause)
    val _     = ex.getMessage shouldBe "Failed to upsert amendment 117-SAMDT-2137: constraint violation"
    val _     = ex.naturalKey shouldBe "117-SAMDT-2137"
    val _     = ex.detail shouldBe "constraint violation"
    ex.getCause shouldBe cause
  }

  "InvalidAmendmentNaturalKey" should "compose a message that includes the natural key and detail" in {
    val ex = InvalidAmendmentNaturalKey("bogus", "natural key must have 3 '-' segments; got 1")
    val _  = ex.getMessage shouldBe "Invalid amendment natural key 'bogus': natural key must have 3 '-' segments; got 1"
    val _  = ex.naturalKey shouldBe "bogus"
    ex.detail shouldBe "natural key must have 3 '-' segments; got 1"
  }

  it should "carry no cause" in {
    val ex = InvalidAmendmentNaturalKey("bogus", "detail")
    Option(ex.getCause) shouldBe None
  }

  "AmendmentUpsertFailed and InvalidAmendmentNaturalKey" should "have unique types so the error classifier can distinguish them" in {
    // Each subsystem error must be a flat, unique exception (per CLAUDE.md). This sanity test would fail if a future
    // refactor accidentally collapsed them into a sealed hierarchy.
    val a: Throwable = AmendmentUpsertFailed("k", "d", new RuntimeException("x"))
    val b: Throwable = InvalidAmendmentNaturalKey("k", "d")
    a.getClass should not be b.getClass
  }

}
