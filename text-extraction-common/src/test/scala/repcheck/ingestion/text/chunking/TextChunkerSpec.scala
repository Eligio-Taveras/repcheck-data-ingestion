package repcheck.ingestion.text.chunking

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import fs2.Stream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Specs for the streaming `TextChunker.chunkPipe` Pipe. Phase 3 of the bill-text-10mb plan replaces the buffered
 * `chunk(text, max)` function with a `Stream[F, String] => Stream[F, String]` Pipe that accumulates incoming fragments
 * into a buffer and emits fixed-size chunks as the buffer fills.
 *
 * Tests materialize streams via `compile.toList` so we can assert the full chunk sequence; in production usage the
 * stream is consumed lazily (one chunk pulled per embedding-batch demand).
 */
class TextChunkerSpec extends AnyFlatSpec with Matchers {

  private def runPipe(input: List[String], maxChunkChars: Int): List[String] =
    Stream.emits(input).covary[IO].through(TextChunker.chunkPipe(maxChunkChars)).compile.toList.unsafeRunSync()

  "TextChunker.chunkPipe" should "produce an empty stream for an empty input stream regardless of size" in {
    val _ = runPipe(Nil, 100) shouldBe Nil
    runPipe(Nil, 1) shouldBe Nil
  }

  it should "raise InvalidChunkSize when maxChunkChars is zero" in {
    val raised = intercept[InvalidChunkSize] {
      runPipe(List("any text"), 0)
    }
    raised.maxChunkChars shouldBe 0
  }

  it should "raise InvalidChunkSize when maxChunkChars is negative" in {
    val raised = intercept[InvalidChunkSize] {
      runPipe(List("any text"), -5)
    }
    raised.maxChunkChars shouldBe -5
  }

  it should "emit a single chunk when the entire input fits inside maxChunkChars" in {
    runPipe(List("short"), 100) shouldBe List("short")
  }

  it should "emit a single chunk when the input length exactly equals maxChunkChars" in {
    runPipe(List("1234567890"), 10) shouldBe List("1234567890")
  }

  it should "concatenate adjacent fragments before chunking" in {
    // Three small fragments concatenate to "abcdefghij" (10 chars). With maxChunkChars=10 we get
    // exactly one chunk equal to the concatenation.
    runPipe(List("abc", "def", "ghij"), 10) shouldBe List("abcdefghij")
  }

  it should "split a single oversized fragment into consecutive fixed-size chunks" in {
    val chunks = runPipe(List("0123456789ABCDEFGHIJ"), 5)
    val _      = chunks should have size 4
    val _      = chunks(0) shouldBe "01234"
    val _      = chunks(1) shouldBe "56789"
    val _      = chunks(2) shouldBe "ABCDE"
    chunks(3) shouldBe "FGHIJ"
  }

  it should "leave the final chunk shorter than maxChunkChars when total input is not a multiple" in {
    val chunks = runPipe(List("0123456789ABC"), 5)
    val _      = chunks should have size 3
    val _      = chunks(0) shouldBe "01234"
    val _      = chunks(1) shouldBe "56789"
    chunks(2) shouldBe "ABC"
  }

  it should "trim each emitted chunk to remove fragment-boundary whitespace" in {
    // Concatenation of fragments with leading/trailing whitespace at boundaries. The chunker's
    // per-chunk trim removes spurious whitespace from each emitted chunk.
    val chunks = runPipe(List(" leading", " middle ", "trailing "), 1000)
    chunks shouldBe List(" leading middle trailing".trim)
  }

  it should "drop chunks that are pure whitespace after trim" in {
    // A fragment of just spaces would emit nothing if the trimmed slice is empty.
    runPipe(List("   ", "   "), 4) shouldBe Nil
  }

  it should "preserve concatenation order across emissions (chunks reconstruct the input modulo whitespace trim)" in {
    val fragments = (1 to 50).map(i => f"$i%03d ").toList
    val chunks    = runPipe(fragments, 17)
    val joined    = chunks.mkString.replaceAll("\\s+", "")
    joined shouldBe fragments.mkString.replaceAll("\\s+", "")
  }

  it should "be deterministic — repeated runs with the same input produce the same output" in {
    val input = List.fill(20)("deterministic input ")
    val once  = runPipe(input, 47)
    val twice = runPipe(input, 47)
    once shouldBe twice
  }

  it should "handle single-character chunks (maxChunkChars = 1)" in {
    runPipe(List("abcd"), 1) shouldBe List("a", "b", "c", "d")
  }

  it should "handle a many-fragment input that produces many chunks of bounded size" in {
    // Many small fragments concatenating to 100,000 chars; chunked at 30,000 gives 4 chunks
    // (3 full + 1 partial).
    val fragments = List.fill(1000)("x" * 100) // 1000 * 100 = 100,000 chars total
    val chunks    = runPipe(fragments, 30_000)
    val _         = chunks should have size 4
    val _         = chunks.take(3).foreach(c => c.length shouldBe 30_000)
    val _         = chunks.lastOption.map(_.length) shouldBe Some(10_000)
    chunks.mkString.length shouldBe 100_000
  }

  it should "not emit a final chunk if the residual buffer trims to empty" in {
    // Fragments that end with only whitespace residual after the last full chunk — final emit
    // should be suppressed so we don't store empty rows in raw_bill_text.
    val chunks = runPipe(List("AAAAA  "), 5)
    chunks shouldBe List("AAAAA")
  }

}
