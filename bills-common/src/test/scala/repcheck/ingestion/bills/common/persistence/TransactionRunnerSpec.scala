package repcheck.ingestion.bills.common.persistence

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransactionRunnerSpec extends AnyFlatSpec with Matchers {

  private val xa: Transactor[IO] =
    Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:txrunner;DB_CLOSE_DELAY=-1",
      user = "sa",
      password = "",
      logHandler = None,
    )

  "TransactionRunner.run" should "execute a ConnectionIO program and return the result" in {
    val program: ConnectionIO[Int] = FC.pure(42)
    val result                     = TransactionRunner.run[IO, Int](xa)(program).unsafeRunSync()
    result shouldBe 42
  }

  it should "execute a composed ConnectionIO for-comprehension atomically" in {
    val program: ConnectionIO[String] = for {
      a <- FC.pure("hello")
      b <- FC.pure(" world")
    } yield a + b
    val result = TransactionRunner.run[IO, String](xa)(program).unsafeRunSync()
    result shouldBe "hello world"
  }

  it should "propagate errors from a failed ConnectionIO program" in {
    val program: ConnectionIO[Int] = FC.raiseError(new RuntimeException("db failure"))
    val result                     = TransactionRunner.run[IO, Int](xa)(program).attempt.unsafeRunSync()
    val _                          = result.isLeft shouldBe true
    result.left.toOption.getOrElse(fail("expected Left")).getMessage shouldBe "db failure"
  }

}
