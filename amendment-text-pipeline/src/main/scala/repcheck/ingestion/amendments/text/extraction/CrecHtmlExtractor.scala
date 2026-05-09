package repcheck.ingestion.amendments.text.extraction

import cats.effect.Async

import fs2.Stream

import repcheck.ingestion.text.extraction.HtmlStreamExtractorBase

/**
 * Streaming HTML extractor for Congressional Record (CREC) HTML granules emitted by api.govinfo.gov.
 *
 * Subclasses [[HtmlStreamExtractorBase]] (shared across bills + amendments) — only the noise-stripping rules differ.
 * Per §7.6 design + Q18 of planning: the bills extractor would leave CREC running headers, page-number footers, and
 * time-of-day annotations in the extracted text, diluting embedding semantics. This extractor drops them per-fragment
 * via an override of [[transformText]]. Element-level (SAX) suppression via `shouldKeepNode` is NOT used here —
 * `HtmlStreamExtractorBase.shouldKeepNode` only sees the element name (not attributes), and the CREC noise we want to
 * drop is identified by `<div class="hd">`-style attributes rather than by element name. Filtering is therefore
 * text-fragment-only at present.
 *
 * Behavior is empirical: the first iteration is conservative (keep more than less). If embedding quality issues surface
 * in production observation, the rules below are the first place to look.
 *
 * ==Rules (all enforced via `transformText` regex scrubbing)==
 *
 *   - Standalone `Pg S5255` / `Page S5255` page-number footers — stripped per fragment.
 *   - `[[Page S5256]]` inline page references — stripped per fragment.
 *   - Time-of-day annotations like `[Time: 10:30 a.m.]` — stripped per fragment.
 *   - Running-header text (e.g. "CONGRESSIONAL RECORD — SENATE August 1, 2021") — currently NOT stripped. The DOM
 *     structure (`<div class="hd">`) is left intact because `shouldKeepNode` lacks attribute visibility; the running
 *     header text flows through as a regular fragment. If this dilutes embedding quality, a follow-up can either (a)
 *     extend the regex below to match the long-form header pattern, or (b) push attribute-aware filtering into
 *     `HtmlStreamExtractorBase`.
 *   - Speaker tags (`<i>Mr. SCHUMER.</i>`) — KEPT, speaker context is part of the amendment's textual evidence.
 *   - Section markers (`Section 1.`, `(a)`, `(b)(2)`) — KEPT, structural skeleton.
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

  // Captures any run of whitespace (spaces, tabs, newlines) — replaced with a single space after the
  // CREC noise patterns above are stripped, so the leftover gaps don't show up as multi-space artifacts
  // in the embedded chunk text.
  private[extraction] val WhitespaceRunPattern: scala.util.matching.Regex =
    """\s+""".r

  /**
   * Apply the CREC-specific noise removal in sequence. Order matters: strip the time annotations first (they may
   * contain numbers that look like page refs), then page references, then page footers. After stripping, collapse any
   * runs of whitespace (spaces, tabs, newlines) the removal left behind into single spaces, then trim leading/trailing
   * whitespace.
   */
  private[extraction] def stripCrecNoise(raw: String): String = {
    val noTime          = TimeAnnotationPattern.replaceAllIn(raw, "")
    val noPage          = InlinePageRefPattern.replaceAllIn(noTime, "")
    val noFoot          = PageNumberFooterPattern.replaceAllIn(noPage, "")
    val collapsedSpaces = WhitespaceRunPattern.replaceAllIn(noFoot, " ")
    collapsedSpaces.trim
  }

  def extract[F[_]: Async](bytes: Stream[F, Byte]): Stream[F, String] = {
    val extractor = new HtmlStreamExtractorBase[F] {
      override protected def transformText(text: String): String = stripCrecNoise(text)
    }
    extractor.extract(bytes)
  }

}
