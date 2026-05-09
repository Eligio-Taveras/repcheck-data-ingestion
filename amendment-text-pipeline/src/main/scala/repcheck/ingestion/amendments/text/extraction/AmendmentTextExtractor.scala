package repcheck.ingestion.amendments.text.extraction

import cats.effect.Async

import fs2.Stream

import repcheck.ingestion.text.extraction.PdfStreamExtractor

/**
 * Format-dispatched streaming extractor for amendment text granules.
 *
 * Per §7.6 design: only two formats are observed upstream — `"HTML"` and `"PDF"`. HTML uses the new
 * [[CrecHtmlExtractor]] purpose-built for Congressional Record markup; PDF reuses the format-agnostic
 * [[PdfStreamExtractor]] from `text-extraction-common`.
 *
 * Unknown formats raise an error rather than silently degrading. In practice [[AmendmentTextProcessor]] now fails fast
 * on unknown formats before this point is reached, so the unsupported-format branch here is a defense-in-depth — but if
 * it does fire, the caller-supplied `naturalKey` makes the resulting log + ProcessingResult name the actual amendment
 * instead of `<unknown>`.
 */
object AmendmentTextExtractor {

  def extractStream[F[_]: Async](bytes: Stream[F, Byte], formatType: String, naturalKey: String): Stream[F, String] =
    formatType match {
      case "HTML" => CrecHtmlExtractor.extract[F](bytes)
      case "PDF"  => PdfStreamExtractor.extract[F](bytes)
      case other =>
        Stream.raiseError[F](
          repcheck.ingestion.amendments.text.errors
            .AmendmentTextProcessingFailed(
              naturalKey,
              s"Unsupported amendment text formatType: '$other' (expected 'HTML' or 'PDF')",
            )
        )
    }

}
