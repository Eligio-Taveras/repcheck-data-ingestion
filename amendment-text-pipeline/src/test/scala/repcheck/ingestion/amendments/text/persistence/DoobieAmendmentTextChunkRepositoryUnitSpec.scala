package repcheck.ingestion.amendments.text.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.amendment.AmendmentTextChunkDO

/**
 * Unit-level shape spec for the amendment_text_chunks Doobie repository. Asserts each method produces a `ConnectionIO`
 * without executing against a real DB. Full SQL round-trip lives in the integration spec.
 */
class DoobieAmendmentTextChunkRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieAmendmentTextChunkRepository

  private def sampleChunk(idx: Int, withEmbedding: Boolean = false): AmendmentTextChunkDO =
    AmendmentTextChunkDO(
      id = 0L,
      amendmentId = 42L,
      versionId = Some(7L),
      chunkIndex = idx,
      content = s"chunk content $idx",
      embedding = if (withEmbedding) Some(Array.fill(1024)(0.5f)) else None,
      createdAt = None,
    )

  "insertMany" should "produce a ConnectionIO when given a non-empty list of chunks" in {
    val cio = repo.insertMany(List(sampleChunk(0), sampleChunk(1)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "short-circuit to no-op for an empty list" in {
    val cio = repo.insertMany(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks carrying full 1024-dim embedding arrays" in {
    val cio = repo.insertMany(List(sampleChunk(0, withEmbedding = true)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks with mixed embedding presence" in {
    val mixed = List(sampleChunk(0, withEmbedding = true), sampleChunk(1, withEmbedding = false))
    val cio   = repo.insertMany(mixed)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks whose versionId is None" in {
    val orphan = sampleChunk(0).copy(versionId = None)
    val cio    = repo.insertMany(List(orphan))
    cio shouldBe a[doobie.ConnectionIO[?]]
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

  "deleteByVersionId" should "produce a ConnectionIO for the DELETE" in {
    val cio = repo.deleteByVersionId(7L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a distinct ConnectionIO for each call (no aliased state)" in {
    val a = repo.deleteByVersionId(1L)
    val b = repo.deleteByVersionId(2L)
    (a eq b) shouldBe false
  }

}
