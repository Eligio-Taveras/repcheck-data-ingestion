package repcheck.ingestion.amendments.observability

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmendmentMetricsSpec extends AnyFlatSpec with Matchers {

  "AmendmentMetrics.make" should "start every counter at 0" in {
    val m = AmendmentMetrics.make()
    val s = m.snapshot()
    val _ = s.detailFetches shouldBe 0L
    val _ = s.recursionRedundantFetches shouldBe 0L
    val _ = s.orphanResolved shouldBe 0L
    s.recursionDepthExceeded shouldBe 0L
  }

  it should "increment each counter independently" in {
    val m = AmendmentMetrics.make()
    m.incrementDetailFetches()
    m.incrementDetailFetches()
    m.incrementRecursionRedundant()
    m.incrementOrphanResolved()
    m.incrementOrphanResolved()
    m.incrementOrphanResolved()
    m.incrementRecursionDepthExceeded()

    val s = m.snapshot()
    val _ = s.detailFetches shouldBe 2L
    val _ = s.recursionRedundantFetches shouldBe 1L
    val _ = s.orphanResolved shouldBe 3L
    s.recursionDepthExceeded shouldBe 1L
  }

  it should "be safe under concurrent increments" in {
    val m = AmendmentMetrics.make()
    val n = 1000
    val threads = (1 to 4).map { _ =>
      val t = new Thread(() => (1 to n).foreach(_ => m.incrementDetailFetches()))
      t.start()
      t
    }
    threads.foreach(_.join())
    m.snapshot().detailFetches shouldBe (4L * n.toLong)
  }

}
