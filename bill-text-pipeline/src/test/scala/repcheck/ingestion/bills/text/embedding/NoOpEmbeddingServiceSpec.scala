package repcheck.ingestion.bills.text.embedding

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NoOpEmbeddingServiceSpec extends AnyFlatSpec with Matchers {

  private val service = new NoOpEmbeddingService[IO]

  "NoOpEmbeddingService" should "return None for any text" in {
    val result = service.generateEmbedding("Some bill text content").unsafeRunSync()
    result shouldBe None
  }

  it should "return None for empty text" in {
    val result = service.generateEmbedding("").unsafeRunSync()
    result shouldBe None
  }

}
