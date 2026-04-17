package repcheck.ingestion.bills.common.persistence

import java.time.Instant

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.Transactor
import doobie.implicits._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.bill.BillSubjectDO

class DoobieBillSubjectRepositoryUnitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

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

  "buildSubjectRow" should "serialize a subject with no embedding to None string" in {
    val (_, _, embStr, _) = repo.buildSubjectRow(sampleSubject)
    embStr shouldBe None
  }

  it should "serialize a subject with embedding to bracketed string" in {
    val withEmb           = sampleSubject.copy(embedding = Some(Array(0.1f, 0.2f)))
    val (_, _, embStr, _) = repo.buildSubjectRow(withEmb)
    embStr shouldBe Some("[0.1,0.2]")
  }

  it should "preserve billId and subjectName" in {
    val (bId, name, _, _) = repo.buildSubjectRow(sampleSubject)
    val _                 = bId shouldBe 1L
    name shouldBe "Health"
  }

  it should "preserve updateDate" in {
    val (_, _, _, upd) = repo.buildSubjectRow(sampleSubject)
    upd shouldBe sampleSubject.updateDate
  }

  "runInsert" should "produce a ConnectionIO[Int] for a non-empty row list" in {
    val row = (1L, "Health", None: Option[String], Some(Instant.parse("2024-01-15T00:00:00Z")))
    val cio = repo.runInsert(List(row))
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  it should "produce a ConnectionIO[Int] for an empty row list" in {
    val cio = repo.runInsert(List.empty)
    cio shouldBe a[doobie.ConnectionIO[?]]
  }

  "parseSubjectRow" should "reconstruct a BillSubjectDO with no embedding" in {
    val row    = (1L, "Health", None: Option[String], Some(Instant.parse("2024-01-15T00:00:00Z")))
    val result = repo.parseSubjectRow(row)
    val _      = result.billId shouldBe 1L
    val _      = result.subjectName shouldBe "Health"
    result.embedding shouldBe None
  }

  it should "reconstruct a BillSubjectDO with an embedding" in {
    val row    = (1L, "Health", Some("[0.1,0.2,0.3]"), None)
    val result = repo.parseSubjectRow(row)
    val arr    = result.embedding.getOrElse(fail("Expected embedding"))
    arr.length shouldBe 3
  }

  "parseEmbedding" should "return None when given None" in {
    repo.parseEmbedding(None) shouldBe None
  }

  it should "parse a bracketed comma-separated embedding string" in {
    val result = repo.parseEmbedding(Some("[0.1,0.2,0.3]"))
    val arr    = result.getOrElse(fail("Expected Some"))
    val _      = arr.length shouldBe 3
    val _      = arr(0) shouldBe 0.1f +- 0.0001f
    val _      = arr(1) shouldBe 0.2f +- 0.0001f
    arr(2) shouldBe 0.3f +- 0.0001f
  }

  it should "return an empty array for an empty bracket pair" in {
    val result = repo.parseEmbedding(Some("[]"))
    result.getOrElse(fail("Expected Some")).length shouldBe 0
  }

  it should "parse a single-element embedding" in {
    val result = repo.parseEmbedding(Some("[1.5]"))
    val arr    = result.getOrElse(fail("Expected Some"))
    val _      = arr.length shouldBe 1
    arr(0) shouldBe 1.5f +- 0.0001f
  }

  it should "handle embeddings with whitespace around values" in {
    val result = repo.parseEmbedding(Some("[0.1, 0.2, 0.3]"))
    val arr    = result.getOrElse(fail("Expected Some"))
    val _      = arr.length shouldBe 3
    arr(0) shouldBe 0.1f +- 0.0001f
  }

  it should "preserve negative values" in {
    val result = repo.parseEmbedding(Some("[-0.5,0.0,0.5]"))
    val arr    = result.getOrElse(fail("Expected Some"))
    val _      = arr.length shouldBe 3
    arr(0) shouldBe -0.5f +- 0.0001f
  }

  private lazy val h2xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:testBillSubject;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val _ = sql"""
      CREATE TABLE IF NOT EXISTS bill_subjects (
        id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        bill_id      BIGINT NOT NULL,
        subject_name TEXT NOT NULL,
        embedding    TEXT,
        update_date  TIMESTAMP WITH TIME ZONE
      )
    """.update.run.transact(h2xa).unsafeRunSync()
    val _ = sql"INSERT INTO bill_subjects (bill_id, subject_name) VALUES (1, 'Health')".update.run
      .transact(h2xa)
      .unsafeRunSync()
  }

  "replaceAll" should "execute delete and insert steps when run with a transactor" in {
    repo.replaceAll(99L, List.empty).transact(h2xa).unsafeRunSync() shouldBe ()
  }

  "findByBillId" should "invoke the row-mapping function when rows exist in H2" in {
    val results = repo.findByBillId(1L).transact(h2xa).unsafeRunSync()
    val _       = results.size shouldBe 1
    results.headOption.map(_.subjectName) shouldBe Some("Health")
  }

}
