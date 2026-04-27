package repcheck.ingestion.bills.text.extraction

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.file.{Path => NioPath}
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Async, Resource}

import fs2.Stream

import org.ccil.cowan.tagsoup.Parser
import org.xml.sax.{Attributes, ContentHandler, InputSource, Locator}

/**
 * Streaming HTML extractor backed by TagSoup, a SAX-based parser that gracefully handles malformed markup. Congress.gov
 * "Formatted Text" payloads occasionally contain stray entities and unbalanced tags inside the bill's `<pre>` block;
 * TagSoup tolerates these where Jsoup's DOM mode can stumble.
 *
 * ==Streaming via push-pull bridge==
 *
 * SAX parsers are inherently push-based — the parser invokes `ContentHandler` callbacks as it walks the document. fs2
 * is pull-based. The bridge: a bounded `LinkedBlockingQueue[Option[String]]` between the parser thread (running on
 * `Async[F].blocking`) and the consuming stream. The parser `put`s text fragments into the queue (blocking when full →
 * natural backpressure if the consumer is slower than the parser). The fs2 `Stream` pulls via `queue.take` in
 * `blocking`. End-of-document is signalled by `put(None)`. The queue is bounded (16 fragments) so the parser never runs
 * ahead of the consumer by more than ~16 fragments worth of text.
 *
 * Error propagation: parser exceptions surface via `Async[F].blocking`'s F effect channel and are routed through fs2's
 * `concurrently` combinator — when the producer fiber fails the consumer is cancelled and the error propagates upward
 * without needing an in-band error sentinel.
 *
 * ==Heap profile==
 *
 * Heap is bounded by `(TagSoup parser internal state) + (queue capacity * average fragment size) + (1 fragment in
 * flight downstream)`. Empirically TagSoup's parser holds a few KB; the queue at 16 * ~1 KB per fragment is ~16 KB;
 * total ~64 KB regardless of body size.
 *
 * ==What we extract==
 *
 * Text content from inside `<body>`, excluding `<script>` and `<style>` subtrees. This covers the Congress.gov
 * `<body><pre>...bill text...</pre></body>` shape and also any future format change that drops the `<pre>` (in which
 * case we'd extract whatever is in the body). Sibling content of `<body>` (`<head>`, `<title>`, etc.) is ignored.
 */
object HtmlStreamExtractor {

  /**
   * Streaming extraction of HTML body text. Spawns a parser fiber that walks the HTML and pushes text fragments into an
   * internal queue; the returned stream pulls those fragments. Both producer and consumer release deterministically via
   * fs2 Resource semantics; a parser failure cancels the consumer and propagates upward.
   *
   * @param path
   *   absolute path to the on-disk HTML download (UTF-8 expected; TagSoup detects encoding from `<meta>` tags or falls
   *   back to UTF-8 — Congress.gov is consistently UTF-8).
   */
  def extract[F[_]: Async](path: NioPath): Stream[F, String] =
    Stream.resource(queueResource[F]).flatMap { queue =>
      val parser =
        Stream
          .eval(Async[F].blocking(runParser(path, queue)))
          .onFinalize(Async[F].blocking { val _ = queue.put(None); () })
          .drain

      val consumer = Stream
        .repeatEval(Async[F].blocking(queue.take()))
        .takeWhile(_.isDefined)
        .map(_.getOrElse(""))
        .filter(_.nonEmpty)

      consumer.concurrently(parser)
    }

  private def queueResource[F[_]: Async]: Resource[F, LinkedBlockingQueue[Option[String]]] =
    Resource.make(Async[F].delay(new LinkedBlockingQueue[Option[String]](16)))(_ => Async[F].unit)

  /**
   * Synchronously parse the HTML file with TagSoup, pushing each text fragment into the queue. Blocks on `queue.put` if
   * the queue is full — that's the backpressure mechanism. Parser failures propagate as a thrown exception out of the
   * calling `Async[F].blocking`, where fs2's `concurrently` re-routes them into the consumer-side error channel.
   */
  private def runParser(path: NioPath, queue: LinkedBlockingQueue[Option[String]]): Unit = {
    val parser  = new Parser()
    val handler = new TextEmittingHandler(queue)
    parser.setContentHandler(handler)
    val byteStream = new BufferedInputStream(new FileInputStream(path.toFile))
    val source     = new InputSource(byteStream)
    try
      parser.parse(source)
    finally
      byteStream.close()
  }

  /**
   * SAX `ContentHandler` that emits `characters` events as queue puts whenever we're inside `<body>` and outside
   * `<script>` / `<style>`. Element nesting is tracked via `AtomicInteger` counters because TagSoup invokes the handler
   * from a single parser thread, but the JVM Memory Model makes the visibility into adjacent SAX callbacks easier to
   * reason about with explicit atomics. Element names are captured lower-case (TagSoup normalizes HTML element names
   * but defensive `.toLowerCase` covers other SAX implementations of this same interface).
   */
  final private class TextEmittingHandler(queue: LinkedBlockingQueue[Option[String]]) extends ContentHandler {
    private val bodyDepth   = new AtomicInteger(0)
    private val scriptDepth = new AtomicInteger(0)
    private val styleDepth  = new AtomicInteger(0)

    override def setDocumentLocator(locator: Locator): Unit                          = ()
    override def startDocument(): Unit                                               = ()
    override def endDocument(): Unit                                                 = ()
    override def startPrefixMapping(prefix: String, uri: String): Unit               = ()
    override def endPrefixMapping(prefix: String): Unit                              = ()
    override def ignorableWhitespace(ch: Array[Char], start: Int, length: Int): Unit = ()
    override def processingInstruction(target: String, data: String): Unit           = ()
    override def skippedEntity(name: String): Unit                                   = ()

    override def startElement(uri: String, localName: String, qName: String, atts: Attributes): Unit = {
      val name = elementName(localName, qName)
      name match {
        case "body"   => val _ = bodyDepth.incrementAndGet()
        case "script" => val _ = scriptDepth.incrementAndGet()
        case "style"  => val _ = styleDepth.incrementAndGet()
        case _        => ()
      }
    }

    override def endElement(uri: String, localName: String, qName: String): Unit = {
      val name = elementName(localName, qName)
      name match {
        case "body"   => if (bodyDepth.get() > 0) { val _ = bodyDepth.decrementAndGet() }
        case "script" => if (scriptDepth.get() > 0) { val _ = scriptDepth.decrementAndGet() }
        case "style"  => if (styleDepth.get() > 0) { val _ = styleDepth.decrementAndGet() }
        case _        => ()
      }
    }

    override def characters(ch: Array[Char], start: Int, length: Int): Unit =
      if (bodyDepth.get() > 0 && scriptDepth.get() == 0 && styleDepth.get() == 0) {
        val raw       = new String(ch, start, length)
        val collapsed = BillTextExtractor.collapseWhitespace(raw)
        if (collapsed.nonEmpty) queue.put(Some(collapsed))
      }

    private def elementName(localName: String, qName: String): String = {
      val raw =
        if (localName != null && localName.nonEmpty) localName
        else if (qName != null) qName
        else ""
      raw.toLowerCase
    }

  }

}
