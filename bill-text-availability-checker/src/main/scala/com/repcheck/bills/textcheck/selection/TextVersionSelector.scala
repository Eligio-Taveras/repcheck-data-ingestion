package com.repcheck.bills.textcheck.selection

import repcheck.shared.models.congress.dto.bill.{FormatDTO, TextVersionDTO}

object TextVersionSelector {

  private val FormatPriority: Map[String, Int] = Map(
    "Formatted Text" -> 0,
    "Formatted XML"  -> 1,
    "PDF"            -> 2,
  )

  final case class SelectedVersion(
    date: Option[String],
    versionType: Option[String],
    formatType: String,
    url: String,
  )

  def selectBestVersion(versions: List[TextVersionDTO]): Option[SelectedVersion] = {
    val candidates: List[(Int, TextVersionDTO, FormatDTO)] = for {
      version  <- versions
      formats  <- version.formats.toList
      format   <- formats
      priority <- FormatPriority.get(format.type_).toList
    } yield (priority, version, format)

    if (candidates.isEmpty) {
      None
    } else {
      // Primary: latest date wins (newer bills have updated text)
      // Secondary: within the same date, use format priority as tiebreaker
      candidates
        .sortBy {
          case (priority, version, _) =>
            (version.date.getOrElse(""), -priority)
        }
        .lastOption
        .map {
          case (_, version, format) =>
            SelectedVersion(
              date = version.date,
              versionType = version.type_,
              formatType = format.type_,
              url = format.url,
            )
        }
    }
  }

}
