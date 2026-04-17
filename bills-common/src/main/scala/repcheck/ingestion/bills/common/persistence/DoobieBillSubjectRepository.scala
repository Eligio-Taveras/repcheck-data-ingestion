package repcheck.ingestion.bills.common.persistence

import cats.syntax.all._

import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.bill.BillSubjectDO

class DoobieBillSubjectRepository extends BillSubjectRepository[ConnectionIO] {

  private val table = Fragment.const(Tables.BillSubjects)

  override def replaceAll(billId: Long, subjects: List[BillSubjectDO]): ConnectionIO[Unit] = {
    val delete = sql"DELETE FROM $table WHERE bill_id = $billId".update.run
    val rows   = subjects.map(buildSubjectRow)
    delete *> runInsert(rows).void
  }

  private[persistence] def buildSubjectRow(
    s: BillSubjectDO
  ): (Long, String, Option[String], Option[java.time.Instant]) = {
    val embeddingStr = s.embedding.map(arr => arr.mkString("[", ",", "]"))
    (s.billId, s.subjectName, embeddingStr, s.updateDate)
  }

  private[persistence] def runInsert(
    rows: List[(Long, String, Option[String], Option[java.time.Instant])]
  ): ConnectionIO[Int] = {
    val insert = Update[(Long, String, Option[String], Option[java.time.Instant])](
      s"INSERT INTO ${Tables.BillSubjects} (bill_id, subject_name, embedding, update_date) VALUES (?, ?, ?::vector, ?)"
    )
    insert.updateMany(rows)
  }

  override def findByBillId(billId: Long): ConnectionIO[List[BillSubjectDO]] =
    sql"SELECT bill_id, subject_name, embedding::text, update_date FROM $table WHERE bill_id = $billId"
      .query[(Long, String, Option[String], Option[java.time.Instant])]
      .map(parseSubjectRow)
      .to[List]

  private[persistence] def parseSubjectRow(
    row: (Long, String, Option[String], Option[java.time.Instant])
  ): BillSubjectDO = {
    val (bId, name, embStr, upd) = row
    BillSubjectDO(billId = bId, subjectName = name, embedding = parseEmbedding(embStr), updateDate = upd)
  }

  private[persistence] def parseEmbedding(embStr: Option[String]): Option[Array[Float]] =
    embStr.map { s =>
      val trimmed = s.stripPrefix("[").stripSuffix("]")
      if (trimmed.isEmpty) Array.empty[Float]
      else trimmed.split(",").map(_.trim.toFloat)
    }

}
