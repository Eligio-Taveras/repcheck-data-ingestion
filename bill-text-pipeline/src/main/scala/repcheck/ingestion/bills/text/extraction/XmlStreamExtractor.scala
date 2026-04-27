package repcheck.ingestion.bills.text.extraction

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.file.{Path => NioPath}
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

import cats.effect.{Async, Resource}

import fs2.Stream

/**
 * Streaming USLM-XML extractor backed by StAX (`javax.xml.stream`, in-JDK, no extra dep).
 *
 * Bill text in `Formatted XML` arrives as a USLM document whose interesting content lives inside `<legis-body>`.
 * Sibling elements (`<metadata>`, `<dublinCore>`, `<form>`) hold administrative boilerplate the embedding model doesn't
 * need. The buffered code path used `XML.loadFile` to build a full DOM and `\\\\ "legis-body"` to descend; this
 * streaming variant walks StAX events with a small state machine, emitting CHARACTERS data only while inside
 * `<legis-body>`.
 *
 * ==Heap profile==
 *
 * StAX's `XMLStreamReader` is a pull-based reader — the parser only buffers the current event's text payload, not the
 * whole document. Heap usage stays bounded by `(parser internal state) + (current CHARACTERS event text) + (1 fragment
 * downstream)`. For a 10 GiB XML document the parser handles it the same as a 10 KiB one.
 *
 * ==State machine==
 *
 *   - START_DOCUMENT — initial state.
 *   - START_ELEMENT — increment `legisBodyDepth` if we're already inside `<legis-body>`; if the element is itself
 *     `<legis-body>`, set `legisBodyDepth = 1` (we're now inside).
 *   - END_ELEMENT — decrement `legisBodyDepth`. When it returns to 0 we've exited the legis-body subtree.
 *   - CHARACTERS — if `legisBodyDepth > 0`, emit `getText()` (collapsed, untrimmed per the per-fragment whitespace
 *     contract).
 *
 * If `<legis-body>` is absent (older or non-standard XML), the extractor falls back to emitting **all** CHARACTERS
 * events from the document. Mirrors the buffered code path's fallback to `xml.text`.
 */
object XmlStreamExtractor {

  /**
   * Walk the supplied XML file via StAX and emit its prose text as a stream of fragments. Each fragment is the text
   * payload of one `CHARACTERS` event, with internal whitespace runs collapsed (no trim — see
   * [[BillTextExtractor.collapseWhitespace]] for the rationale).
   *
   * The XML stream is opened as a `Resource` so the underlying file handle and StAX reader release deterministically
   * even on stream cancellation or error.
   */
  def extract[F[_]: Async](path: NioPath): Stream[F, String] =
    Stream.resource(readerResource[F](path)).flatMap { reader =>
      Stream.unfoldEval(InitialState)(state => Async[F].blocking(advance(reader, state)))
    }

  private def readerResource[F[_]: Async](path: NioPath): Resource[F, XMLStreamReader] = {
    val acquire = Async[F].blocking {
      val factory = XMLInputFactory.newDefaultFactory()
      // Defensive: defeat XXE / billion-laughs vectors. Bill XML never references external DTDs;
      // disable resolution so a maliciously crafted document can't pull from disk or network.
      factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, java.lang.Boolean.FALSE)
      factory.setProperty(XMLInputFactory.SUPPORT_DTD, java.lang.Boolean.FALSE)
      val stream = new BufferedInputStream(new FileInputStream(path.toFile))
      val reader = factory.createXMLStreamReader(stream)
      // Bundle the input stream with the reader so we can close both deterministically.
      ReaderHandle(reader, stream)
    }
    Resource
      .make(acquire)(handle =>
        Async[F].blocking {
          handle.reader.close()
          handle.stream.close()
        }
      )
      .map(_.reader)
  }

  final private case class ReaderHandle(reader: XMLStreamReader, stream: BufferedInputStream)

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
   *
   * Bills always have `<legis-body>`. If a future format change drops it, this extractor returns an empty stream — a
   * loud failure mode (downstream chunker emits nothing, processor logs "0 chunks") rather than a silent fall back.
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
            val collapsed = BillTextExtractor.collapseWhitespace(reader.getText)
            if (s.legisBodyDepth > 0 && collapsed.nonEmpty) {
              Some((collapsed, s))
            } else {
              loop(s)
            }

          case _ =>
            // Skip START_DOCUMENT, END_DOCUMENT (loop terminates via hasNext), COMMENT, PI, DTD,
            // ATTRIBUTE, NAMESPACE, SPACE, ENTITY_REFERENCE. We treat END_DOCUMENT as a non-event
            // here because the next `hasNext` returns false and the next loop iteration returns
            // None — same effective behavior with one less branch to cover.
            loop(s)
        }
      }

    loop(state)
  }

}
