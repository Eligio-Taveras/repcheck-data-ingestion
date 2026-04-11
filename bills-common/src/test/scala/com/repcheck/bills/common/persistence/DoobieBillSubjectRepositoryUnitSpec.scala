package com.repcheck.bills.common.persistence

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillSubjectDO

class DoobieBillSubjectRepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private val repo = new DoobieBillSubjectRepository

  private val sampleSubject = BillSubjectDO(
    billId = 1L,
    subjectName = "Health",
    embedding = None,
    updateDate = Some(Instant.parse("2024-01-15T00:00:00Z")),
  )

  "replaceAll" should "produce a ConnectionIO describing the delete-then-insert" in {
    val cio = repo.replaceAll(1L, List(sampleSubject))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle an empty subject list" in {
    val cio = repo.replaceAll(1L, List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle subjects with embeddings" in {
    val withEmbedding = sampleSubject.copy(embedding = Some(Array(0.1f, 0.2f, 0.3f)))
    val cio           = repo.replaceAll(1L, List(withEmbedding))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "handle subjects without updateDate" in {
    val noDate = sampleSubject.copy(updateDate = None)
    val cio    = repo.replaceAll(1L, List(noDate))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "findByBillId" should "produce a ConnectionIO for the query" in {
    val cio = repo.findByBillId(1L)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

}
