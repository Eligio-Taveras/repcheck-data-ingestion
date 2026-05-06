package repcheck.ingestion.text.extraction

import cats.effect.Async

import fs2.Stream

/**
 * Default [[HtmlStreamExtractorBase]] used by the bill-text-pipeline. Inherits the base's body/script/style filtering
 * with no additional `shouldKeepNode` or `transformText` customizations. Other consumers (e.g. amendments-pipeline for
 * CREC headers/footers) build their own object/class extending [[HtmlStreamExtractorBase]] with overrides.
 */
object BillsHtmlStreamExtractor {

  def extract[F[_]: Async](bytes: Stream[F, Byte]): Stream[F, String] = {
    val extractor = new HtmlStreamExtractorBase[F] {}
    extractor.extract(bytes)
  }

}
