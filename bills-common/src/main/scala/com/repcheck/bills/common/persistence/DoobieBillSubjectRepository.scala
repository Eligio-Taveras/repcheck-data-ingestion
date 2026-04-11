package com.repcheck.bills.common.persistence

import doobie._
import doobie.implicits._

import repcheck.pipeline.models.constants.Tables
import repcheck.shared.models.congress.dos.bill.BillSubjectDO

class DoobieBillSubjectRepository extends BillSubjectRepository[ConnectionIO] {

  private val table = Fragment.const(Tables.BillSubjects)

  override def replaceAll(billId: Long, subjects: List[BillSubjectDO]): ConnectionIO[Unit] = {
    val delete = sql"DELETE FROM $table WHERE bill_id = $billId".update.run

    val insert = Update[(Long, String, Option[String], Option[String])](
      s"INSERT INTO ${Tables.BillSubjects} (bill_id, subject_name, embedding, update_date) VALUES (?, ?, ?::vector, ?::timestamptz)"
    )

    val rows = subjects.map { s =>
      val embeddingStr = s.embedding.map(arr => arr.mkString("[", ",", "]"))
      (s.billId, s.subjectName, embeddingStr, s.updateDate)
    }

    for {
      _ <- delete
      _ <- insert.updateMany(rows)
    } yield ()
  }

  override def findByBillId(billId: Long): ConnectionIO[List[BillSubjectDO]] =
    sql"SELECT bill_id, subject_name, embedding::text, update_date::text FROM $table WHERE bill_id = $billId"
      .query[(Long, String, Option[String], Option[String])]
      .map {
        case (bId, name, embStr, upd) =>
          val embedding = embStr.map { s =>
            val trimmed = s.stripPrefix("[").stripSuffix("]")
            if (trimmed.isEmpty) { Array.empty[Float] }
            else { trimmed.split(",").map(_.trim.toFloat) }
          }
          BillSubjectDO(billId = bId, subjectName = name, embedding = embedding, updateDate = upd)
      }
      .to[List]

}
