package repcheck.ingestion.text.extraction

import java.io.InputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

import cats.effect.{Async, Resource}

import fs2.Stream

/**
 * Streaming USLM-XML extractor backed by StAX (`javax.xml.stream`, in-JDK, no extra dep). Consumes a `Stream[F, Byte]`
 * (typically the HTTP response body) directly — no temp file required.
 *
 * Bill text in `Formatted XML` arrives as a USLM document whose interesting content lives inside `<legis-body>`.
 * Sibling elements (`<metadata>`, `<dublinCore>`, `<form>`) hold administrative boilerplate the embedding model doesn't
 * need. This streaming variant walks StAX events with a small state machine, emitting CHARACTERS data only while inside
 * `<legis-body>`.
 *
 * ==Streaming shape==
 *
 * `fs2.io.toInputStreamResource` exposes the byte stream as a `java.io.InputStream` (needed by StAX's
 * `XMLInputFactory.createXMLStreamReader(InputStream)` constructor). The bridge runs inside an internal queue; StAX's
 * pull-based reads pull from the queue, which pulls from the fs2 stream — backpressure flows end-to-end.
 *
 * ==Heap profile==
 *
 * StAX's `XMLStreamReader` is a pull-based reader — the parser only buffers the current event's text payload, not the
 * whole document. Heap usage stays bounded by `(parser internal state) + (current CHARACTERS event text) + (1 fragment
 * downstream)`. For a 10 GiB XML body the parser handles it the same as a 10 KiB one.
 *
 * ==State machine==
 *
 *   - START_ELEMENT — increment `legisBodyDepth` if element is `<legis-body>` or we're already inside one.
 *   - END_ELEMENT — decrement `legisBodyDepth` if `> 0`.
 *   - CHARACTERS — if `legisBodyDepth > 0`, emit `getText()` (collapsed, untrimmed per the per-fragment whitespace
 *     contract).
 *
 * Bills always have `<legis-body>`. If a future format change drops it, this extractor returns an empty stream — a loud
 * failure mode (downstream chunker emits nothing, processor logs "0 chunks") rather than a silent fall back.
 *
 * ==XXE / billion-laughs hardening==
 *
 * External-entity resolution and DTD support are explicitly disabled on the `XMLInputFactory` so a maliciously crafted
 * document can't pull from disk or network or expand recursive entities to exhaust memory.
 */
object XmlStreamExtractor {

  /**
   * Walk the supplied byte stream as XML via StAX and emit its prose text as a stream of fragments. Each fragment is
   * the text payload of one `CHARACTERS` event, with internal whitespace runs collapsed (no trim — see
   * [[TextExtractor.collapseWhitespace]] for the rationale).
   *
   * The InputStream and the StAX reader are both held in `Resource`s so the JVM resources release deterministically
   * even on stream cancellation or error.
   */
  def extract[F[_]: Async](bytes: Stream[F, Byte]): Stream[F, String] =
    Stream.resource(fs2.io.toInputStreamResource(bytes)).flatMap { is =>
      Stream.resource(readerResource[F](is)).flatMap { reader =>
        Stream.unfoldEval(InitialState)(state => Async[F].blocking(advance(reader, state)))
      }
    }

  private def readerResource[F[_]: Async](is: InputStream): Resource[F, XMLStreamReader] =
    Resource.make(
      Async[F].blocking {
        val factory = XMLInputFactory.newDefaultFactory()
        // Defeat XXE / billion-laughs vectors. Bill XML never references external DTDs;
        // disable resolution so a maliciously crafted document can't pull from disk or network.
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, java.lang.Boolean.FALSE)
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, java.lang.Boolean.FALSE)
        factory.createXMLStreamReader(is)
      }
    )(reader => Async[F].blocking(reader.close()))

  /**
   * Streaming state across StAX events.
   *
   * @param legisBodyDepth
   *   nesting depth inside the current `<legis-body>` subtree. 0 = outside, 1 = directly inside, >1 = nested element.
   *   Text only emits when `> 0`.
   */
  final private case class State(legisBodyDepth: Int)

  private val InitialState: State = State(legisBodyDepth = 0)

  /**
   * Pull the next CHARACTERS payload to emit, walking StAX events until we find one or hit END_DOCUMENT. Returns
   * `Some((fragment, nextState))` to emit a fragment and continue, or `None` to signal end-of-stream.
   */
  private def advance(reader: XMLStreamReader, state: State): Option[(String, State)] = {
    @scala.annotation.tailrec
    def loop(s: State): Option[(String, State)] =
      if (!reader.hasNext) {
        None
      } else {
        val event = reader.next()
        event match {
          case XMLStreamConstants.START_ELEMENT =>
            val name = reader.getLocalName
            if (name == "legis-body") {
              loop(s.copy(legisBodyDepth = s.legisBodyDepth + 1))
            } else if (s.legisBodyDepth > 0) {
              loop(s.copy(legisBodyDepth = s.legisBodyDepth + 1))
            } else {
              loop(s)
            }

          case XMLStreamConstants.END_ELEMENT =>
            if (s.legisBodyDepth > 0) {
              loop(s.copy(legisBodyDepth = s.legisBodyDepth - 1))
            } else {
              loop(s)
            }

          case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
            val collapsed = TextExtractor.collapseWhitespace(reader.getText)
            if (s.legisBodyDepth > 0 && collapsed.nonEmpty) {
              Some((collapsed, s))
            } else {
              loop(s)
            }

          case _ =>
            // Skip START_DOCUMENT, END_DOCUMENT (loop terminates via hasNext), COMMENT, PI, DTD,
            // ATTRIBUTE, NAMESPACE, SPACE, ENTITY_REFERENCE.
            loop(s)
        }
      }

    loop(state)
  }

}
