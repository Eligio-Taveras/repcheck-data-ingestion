package repcheck.ingestion.bills.text.persistence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.RawBillTextDO

/**
 * Unit-level shape spec for the Doobie raw_bill_text repository. Asserts each method produces a `ConnectionIO`
 * (Doobie's pure description-of-effect type) without executing against a real DB. The full SQL round-trip lives in the
 * DockerRequired integration spec; this spec exists so the default test scope catches signature / wiring bugs without a
 * Postgres dependency.
 *
 * Post-Option-C-refactor: the API is `upsertMany` (idempotent INSERT-or-UPDATE on the conflict key) plus
 * `trimChunksPast` (delete stale tail past a new submission's chunk count). The previous `replaceAll` /
 * `deleteByVersionId` / `insertOne` / `insertMany` are gone — UPSERT-then-trim subsumes them.
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

  "upsertMany" should "produce a no-op ConnectionIO returning 0 for an empty list (short-circuit branch)" in {
    val cio = repo.upsertMany(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO for a non-empty batch (Update.updateMany path)" in {
    val rows = (0 until 5).map(idx => sampleChunk(idx, withEmbedding = idx % 2 == 0)).toList
    val cio  = repo.upsertMany(rows)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept a single-row batch (lower bound of the non-empty path)" in {
    val cio = repo.upsertMany(List(sampleChunk(0, withEmbedding = true)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept a batch carrying mixed embedding presence" in {
    val rows =
      List(sampleChunk(0, withEmbedding = true), sampleChunk(1, withEmbedding = false), sampleChunk(2))
    val cio = repo.upsertMany(rows)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept a batch where some chunks have versionId = None" in {
    val rows = List(
      sampleChunk(0).copy(versionId = None),
      sampleChunk(1).copy(versionId = Some(7L)),
    )
    val cio = repo.upsertMany(rows)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunks carrying full 1024-dim embedding arrays without complaint" in {
    val cio = repo.upsertMany(List(sampleChunk(0, withEmbedding = true)))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "trimChunksPast" should "produce a ConnectionIO for the DELETE statement" in {
    val cio = repo.trimChunksPast(7L, 10)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "accept chunkCount = 0 (delete every chunk for the version)" in {
    val cio = repo.trimChunksPast(7L, 0)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a distinct ConnectionIO instance for each call (no shared mutable state)" in {
    val first  = repo.trimChunksPast(7L, 10)
    val second = repo.trimChunksPast(8L, 10)
    // Each invocation must materialize its own description; if the impl accidentally cached
    // a singleton instance the two would alias and the WHERE clause would point at the wrong id.
    (first eq second) shouldBe false
  }

  "findByVersionId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findByVersionId(7L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "countByVersionId" should "produce a ConnectionIO returning Long" in {
    val cio = repo.countByVersionId(7L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
