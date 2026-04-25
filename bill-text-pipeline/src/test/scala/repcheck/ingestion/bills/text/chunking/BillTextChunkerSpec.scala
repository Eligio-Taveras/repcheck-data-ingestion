package repcheck.ingestion.bills.text.chunking

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BillTextChunkerSpec extends AnyFlatSpec with Matchers {

  "BillTextChunker.chunk" should "return Nil for empty input regardless of size" in {
    val _ = BillTextChunker.chunk("", 100) shouldBe Nil
    val _ = BillTextChunker.chunk("", 0) shouldBe Nil
    BillTextChunker.chunk("", -5) shouldBe Nil
  }

  it should "return Nil defensively for non-positive maxChunkChars on non-empty input" in {
    val _ = BillTextChunker.chunk("any text", 0) shouldBe Nil
    BillTextChunker.chunk("any text", -1) shouldBe Nil
  }

  it should "return a single-element list when text fits inside maxChunkChars" in {
    BillTextChunker.chunk("short", 100) shouldBe List("short")
  }

  it should "return a single-element list when text length exactly equals maxChunkChars" in {
    val text = "1234567890"
    BillTextChunker.chunk(text, 10) shouldBe List(text)
  }

  it should "split text into consecutive non-overlapping windows of exactly maxChunkChars" in {
    val text   = "0123456789ABCDEFGHIJ"
    val chunks = BillTextChunker.chunk(text, 5)
    val _      = chunks should have size 4
    val _      = chunks(0) shouldBe "01234"
    val _      = chunks(1) shouldBe "56789"
    val _      = chunks(2) shouldBe "ABCDE"
    chunks(3) shouldBe "FGHIJ"
  }

  it should "leave the final chunk shorter than maxChunkChars when text is not a multiple" in {
    val text   = "0123456789ABC"
    val chunks = BillTextChunker.chunk(text, 5)
    val _      = chunks should have size 3
    val _      = chunks(0) shouldBe "01234"
    val _      = chunks(1) shouldBe "56789"
    chunks(2) shouldBe "ABC"
  }

  it should "preserve exact reconstructibility — joining chunks reproduces the original text" in {
    val text   = "The quick brown fox jumps over the lazy dog. " * 50
    val chunks = BillTextChunker.chunk(text, 37)
    val _      = chunks.size should be > 1
    chunks.mkString shouldBe text
  }

  it should "preserve order across chunk boundaries" in {
    val text   = (1 to 100).map(i => f"$i%03d").mkString
    val chunks = BillTextChunker.chunk(text, 30)
    chunks.mkString shouldBe text
  }

  it should "be deterministic — repeated calls with the same inputs return the same output" in {
    val text  = "deterministic input " * 200
    val once  = BillTextChunker.chunk(text, 47)
    val twice = BillTextChunker.chunk(text, 47)
    once shouldBe twice
  }

  it should "handle single-character chunks (maxChunkChars = 1)" in {
    BillTextChunker.chunk("abcd", 1) shouldBe List("a", "b", "c", "d")
  }

  it should "handle a 14 MB-style oversized input by producing many chunks of bounded size" in {
    // Smaller than the real 14 MB case (kept fast for unit test) but exercises the same path —
    // text far larger than the chunk window producing many ordered slices.
    val text      = "x" * 100_000
    val chunkSize = 30_000
    val chunks    = BillTextChunker.chunk(text, chunkSize)
    val _         = chunks should have size 4 // 30000 + 30000 + 30000 + 10000
    val _         = chunks.take(3).foreach(c => c.length shouldBe chunkSize)
    val _         = chunks.lastOption.map(_.length) shouldBe Some(10_000)
    chunks.mkString shouldBe text
  }

}
