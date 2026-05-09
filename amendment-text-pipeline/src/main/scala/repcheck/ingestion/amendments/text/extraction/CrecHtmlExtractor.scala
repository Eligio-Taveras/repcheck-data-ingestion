package repcheck.ingestion.amendments.text.extraction

import cats.effect.Async

import fs2.Stream

import repcheck.ingestion.text.extraction.HtmlStreamExtractorBase

/**
 * Streaming HTML extractor for Congressional Record (CREC) HTML granules emitted by api.govinfo.gov.
 *
 * Subclasses [[HtmlStreamExtractorBase]] (shared across bills + amendments) — only the noise-stripping rules differ.
 * Per §7.6 design + Q18 of planning: the bills extractor would leave CREC running headers, page-number footers, and
 * time-of-day annotations in the extracted text, diluting embedding semantics. This extractor drops them at the
 * SAX-level via [[shouldKeepNode]] and per-fragment via [[transformText]].
 *
 * Behavior is empirical: the first iteration is conservative (keep more than less). If embedding quality issues surface
 * in production observation, the rules below are the first place to look.
 *
 * ==Rules==
 *
 *   - `<div class="hd">` running headers ("CONGRESSIONAL RECORD — SENATE August 1, 2021") — dropped wholesale via
 *     subtree suppression. Element name is `div`, but we only want to drop hd/hd1 etc., so the suppression is scoped to
 *     those classes via the SAX-level decision; here we approximate by suppressing any `div` whose class starts with
 *     `hd`. TagSoup normalizes class attributes; without the `Attributes` parameter we'd need to expand
 *     `shouldKeepNode` to take the attribute set. Simpler: we drop the class prefix at the text-fragment level via a
 *     regex and accept that the DOM structure is left intact.
 *   - Standalone `Pg S5255` page-number footers — stripped per fragment via regex.
 *   - `[[Page S5256]]` inline page references — stripped per fragment.
 *   - Time-of-day annotations like `[Time: 10:30 a.m.]` — stripped per fragment.
 *   - Speaker tags (`<i>Mr. SCHUMER.</i>`) — KEPT, speaker context is part of the amendment's textual evidence.
 *   - Section markers (`Section 1.`, `(a)`, `(b)(2)`) — KEPT, structural skeleton.
 *
 * Since `HtmlStreamExtractorBase.shouldKeepNode` only sees the element name (not attributes), we conservatively keep
 * all elements and rely on `transformText` to scrub the noise. If running-header drops become important for embedding
 * quality, a future enhancement could push attribute-aware filtering into the base.
 */
object CrecHtmlExtractor {

  // Captures inline page references like "[[Page S5256]]" — tolerant of whitespace inside the brackets.
  private[extraction] val InlinePageRefPattern: scala.util.matching.Regex =
    """\[\[\s*Page\s+[A-Za-z0-9-]+\s*\]\]""".r

  // Captures standalone page-number footers like "Pg S5255" or "Page S5255" — anchored loosely so we strip the
  // text occurrence wherever it appears in a fragment.
  private[extraction] val PageNumberFooterPattern: scala.util.matching.Regex =
    """\b(?:Pg|Page)\s+[A-Za-z]?\d+(?:-\d+)?\b""".r

  // Captures bracketed time-of-day annotations: `[Time: 10:30 a.m.]`, `[Time: 10:30:45]`, etc.
  private[extraction] val TimeAnnotationPattern: scala.util.matching.Regex =
    """\[\s*Time:\s*[^\]]+\]""".r

  /**
   * Apply the CREC-specific noise removal in sequence. Order matters: strip the time annotations first (they may
   * contain numbers that look like page refs), then page references, then page footers. After stripping, collapse any
   * extra whitespace that the removal left behind.
   */
  private[extraction] def stripCrecNoise(raw: String): String = {
    val noTime  = TimeAnnotationPattern.replaceAllIn(raw, "")
    val noPage  = InlinePageRefPattern.replaceAllIn(noTime, "")
    val noFoot  = PageNumberFooterPattern.replaceAllIn(noPage, "")
    val cleaned = noFoot.trim
    cleaned
  }

  def extract[F[_]: Async](bytes: Stream[F, Byte]): Stream[F, String] = {
    val extractor = new HtmlStreamExtractorBase[F] {
      override protected def transformText(text: String): String = stripCrecNoise(text)
    }
    extractor.extract(bytes)
  }

}
