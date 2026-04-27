package repcheck.ingestion.bills.text.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Unit-level shape spec for the Doobie raw_bill_text repository. Asserts each method produces a `ConnectionIO`
 * (Doobie's pure description-of-effect type) without executing against a real DB. The full SQL round-trip lives in the
 * DockerRequired integration spec; this spec exists so the default test scope catches signature / wiring bugs without a
 * Postgres dependency.
 */
class DoobieRawBillTextRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieRawBillTextRepository

  private def sampleChunk(idx: Int, withEmbedding: Boolean = false): RawBillTextDO =
    RawBillTextDO(
      id = 0L,
      billId = 42L,
      versionId = Some(7L),
      chunkIndex = idx,
      content = s"chunk content $idx",
      embedding = if (withEmbedding) Some(Array.fill(1024)(0.5f)) else None,
      createdAt = None,
    )

  "replaceAll" should "produce a ConnectionIO when given a non-empty list of chunks" in {
    val cio = repo.replaceAll(7L, List(sampleChunk(0), sampleChunk(1)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for an empty chunk list (delete-only path)" in {
    val cio = repo.replaceAll(7L, List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks carrying full 1024-dim embedding arrays without complaint" in {
    val cio = repo.replaceAll(7L, List(sampleChunk(0, withEmbedding = true)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks with mixed embedding presence (some Some, some None)" in {
    val mixed = List(sampleChunk(0, withEmbedding = true), sampleChunk(1, withEmbedding = false))
    val cio   = repo.replaceAll(7L, mixed)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks whose versionId is None (orphan chunks before parent linkage)" in {
    val orphan = sampleChunk(0).copy(versionId = None)
    val cio    = repo.replaceAll(7L, List(orphan))
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

  "deleteByVersionId" should "produce a ConnectionIO for the DELETE statement" in {
    val cio = repo.deleteByVersionId(7L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a distinct ConnectionIO instance for each call (no shared mutable state)" in {
    val first  = repo.deleteByVersionId(7L)
    val second = repo.deleteByVersionId(8L)
    // Each invocation must materialize its own description; if the impl accidentally cached
    // a singleton instance the two would alias and the WHERE clause would point at the wrong id.
    (first eq second) shouldBe false
  }

  "insertOne" should "produce a ConnectionIO for a chunk without an embedding" in {
    val cio = repo.insertOne(sampleChunk(0))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a chunk carrying a full 1024-dim embedding" in {
    val cio = repo.insertOne(sampleChunk(0, withEmbedding = true))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept a chunk whose versionId is None (no parent linkage yet)" in {
    val orphan = sampleChunk(0).copy(versionId = None)
    val cio    = repo.insertOne(orphan)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
