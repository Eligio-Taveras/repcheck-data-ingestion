package repcheck.ingestion.votes.metrics

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentPlaceholderSkipCounterSpec extends AnyFlatSpec with Matchers {

  "AmendmentPlaceholderSkipCounter" should "start at zero" in {
    new AmendmentPlaceholderSkipCounter().pre102Skips shouldBe 0L
  }

  it should "increment by one per incPre102Skip invocation" in {
    val counter = new AmendmentPlaceholderSkipCounter
    val _       = counter.incPre102Skip[IO].unsafeRunSync()
    val _       = counter.incPre102Skip[IO].unsafeRunSync()
    counter.pre102Skips shouldBe 2L
  }

  it should "stay independent across instances" in {
    val a = new AmendmentPlaceholderSkipCounter
    val b = new AmendmentPlaceholderSkipCounter
    val _ = a.incPre102Skip[IO].unsafeRunSync()
    val _ = a.pre102Skips shouldBe 1L
    b.pre102Skips shouldBe 0L
  }

  it should "be safe under repeated concurrent increments" in {
    import cats.syntax.all._
    val counter = new AmendmentPlaceholderSkipCounter
    val _       = (1 to 100).toList.parTraverse_(_ => counter.incPre102Skip[IO]).unsafeRunSync()
    counter.pre102Skips shouldBe 100L
  }

}
