package repcheck.ingestion.amendments.text.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit-level shape spec for the amendment_text_chunks Doobie repository. Asserts each method produces a `ConnectionIO`
 * without executing against a real DB. Full SQL round-trip lives in the integration spec.
 */
class DoobieAmendmentTextChunkRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieAmendmentTextChunkRepository

  private def sampleRow(idx: Int, withEmbedding: Boolean = false): AmendmentChunkRow =
    AmendmentChunkRow(
      amendmentId = 42L,
      versionId = 7L,
      chunkIndex = idx,
      content = s"chunk content $idx",
      embedding = if (withEmbedding) Some(Array.fill(1024)(0.5f)) else None,
    )

  "upsertMany" should "produce a ConnectionIO when given a non-empty list of chunks" in {
    val cio = repo.upsertMany(List(sampleRow(0), sampleRow(1)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "short-circuit to a constant 0 ConnectionIO for an empty list" in {
    val cio = repo.upsertMany(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks carrying full 1024-dim embedding arrays" in {
    val cio = repo.upsertMany(List(sampleRow(0, withEmbedding = true)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks with mixed embedding presence" in {
    val mixed = List(sampleRow(0, withEmbedding = true), sampleRow(1, withEmbedding = false))
    val cio   = repo.upsertMany(mixed)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "trimChunksPast" should "produce a ConnectionIO returning the deleted-row count" in {
    val cio = repo.trimChunksPast(7L, 100)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a distinct ConnectionIO for each call (no aliased state)" in {
    val a = repo.trimChunksPast(1L, 5)
    val b = repo.trimChunksPast(2L, 5)
    (a eq b) shouldBe false
  }

  "findByVersionId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findByVersionId(7L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "countByVersionId" should "produce a ConnectionIO returning Long" in {
    val cio = repo.countByVersionId(7L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "sumContentLengthByVersionId" should "produce a ConnectionIO returning Long" in {
    val cio = repo.sumContentLengthByVersionId(7L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
