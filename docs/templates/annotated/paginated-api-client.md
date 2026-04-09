# Pattern: Paginated API Client

## Pattern Summary
A tagless-final trait that abstracts paginated HTTP API calls over any effect type `F[_]`. Uses `Async[F].tailRecM` for stack-safe pagination and `Semigroup[T]` for combining pages. Concrete implementations (e.g., `LegislativeBillsApi`) extend the trait with endpoint-specific defaults.

## When To Use This Pattern
- Any new Congress.gov API endpoint (votes, members, amendments)
- Any external API that returns paginated results
- When you need memory-efficient streaming of large result sets

## Source Files
- `gov-apis/src/main/scala/apiBase/PagingApiBase.scala` — generic trait
- `gov-apis/src/main/scala/congress/gov/apis/LegislativeBillsApi.scala` — concrete implementation

---

## The Generic Pagination Trait

```scala
// File: gov-apis/src/main/scala/apiBase/PagingApiBase.scala

package apiBase

import java.time.ZonedDateTime
import cats.Semigroup
import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.http4s.Uri.Scheme
import org.http4s.client.Client
import org.http4s.{EntityDecoder, Uri}
import fs2.io.net.Network
import apiBase.PagingApiBase.SortOrder
import common.Constants

// ANNOTATION: Unique, flat exception for protocol validation failures.
// Following the project pattern: one exception per error scenario,
// context implied by the executing application.
case class InvalidProtocol(protocol: String)
    extends Exception(s"Invalid protocol: $protocol")

// ANNOTATION: Marker trait for any DTO that can report how many items it contains.
// This is the contract that tells the pagination loop whether to fetch more pages.
trait PagedObject {
  def lengthRetrieved: Int
}

// ANNOTATION: The core pattern. This is a TAGLESS FINAL trait parameterized over:
//   F[_]  — the effect type (IO in production, could be anything with Async)
//   T     — the response DTO type, which must:
//           1. Extend PagedObject (so we know when to stop paginating)
//           2. Have a Semigroup instance (so we can combine pages)
//
// The type constraints [F[_]: Async: Network, T <: PagedObject: Semigroup] mean:
//   - F must have an Async instance (for HTTP calls, delays, error handling)
//   - F must have a Network instance (required by http4s Ember client)
//   - T must extend PagedObject AND have a Semigroup (for page combining)
trait PagingApiBase[F[_]: Async: Network, T <: PagedObject: Semigroup] {

  // ANNOTATION: Protected abstract members define the API CONTRACT.
  // Every concrete implementation MUST provide these. This is how the
  // trait knows how to build the URL and make HTTP calls.
  protected def protocol: String           // "https"
  protected def host: String               // "api.congress.gov"
  protected def apiKey: String             // API authentication key
  protected def path: String               // "/v3/bill"
  protected def pageSize: Int              // Items per page
  implicit protected def decoder: EntityDecoder[F, T]  // How to parse HTTP response → T
  protected def emberClient: Resource[F, Client[F]]    // HTTP client (Resource for lifecycle)

  // ANNOTATION: Public entry point — fetches ALL pages and combines them.
  // Uses sensible defaults (sort by update date descending).
  def defaultCall(
      fromDateTime: Option[ZonedDateTime],
      toDateTime: Option[ZonedDateTime]
  ): F[T] = {
    getAll(
      fromDateTime = fromDateTime,
      toDateTime = toDateTime,
      sortOrder = Some(SortOrder.UpdateDateDesc)
    )
  }

  // ANNOTATION: Public entry point — fetches ONE page as an FS2 Stream.
  // Used by BillIdentifierApp for page-by-page streaming to AlloyDB.
  def streamBatch(
      fromDateTime: Option[ZonedDateTime],
      toDateTime: Option[ZonedDateTime],
      offset: Int = 0,
      sortOrder: Option[SortOrder] = Some(SortOrder.UpdateDateDesc)
  ): fs2.Stream[F, T] = {
    fs2.Stream.eval(
      getObjects(Some(offset), Some(pageSize), fromDateTime, toDateTime, sortOrder)
    )
  }

  // ANNOTATION: STACK-SAFE PAGINATION using Async[F].tailRecM.
  //
  // tailRecM is a monadic loop that avoids stack overflow:
  //   - Returns Left(nextState) to continue looping
  //   - Returns Right(result) to stop
  //
  // The accumulator is (offset, Option[T]):
  //   - offset: current position in the paginated results
  //   - Option[T]: accumulated results so far (None on first page)
  //
  // The Semigroup[T] instance (|+|) combines pages:
  //   e.g., LegislativeBillsDTO(bills1) |+| LegislativeBillsDTO(bills2)
  //       = LegislativeBillsDTO(bills1 ++ bills2)
  private def getAll(
      fromDateTime: Option[ZonedDateTime],
      toDateTime: Option[ZonedDateTime],
      sortOrder: Option[SortOrder]
  ): F[T] = {
    Async[F].tailRecM[(Int, Option[T]), T]((0, None)) { case (offset, acc) =>
      getObjects(Some(offset), Some(pageSize), fromDateTime, toDateTime, sortOrder).flatMap {
        page =>
          // Combine with accumulator using Semigroup
          val combined = acc.fold(page)(_ |+| page)

          // If this page has fewer items than pageSize, we've reached the end
          if (page.lengthRetrieved < pageSize) {
            Async[F].delay {
              println(s"Retrieved ${page.lengthRetrieved} objects")
              System.out.flush()
            }.as(Right(combined))  // Right = stop looping
          } else {
            Async[F].delay {
              println(s"Retrieved ${page.lengthRetrieved} objects," +
                s" accumulated ${combined.lengthRetrieved} objects total so far")
              System.out.flush()
            }.as(Left((offset + pageSize, Some(combined))))  // Left = continue
          }
      }
    }
  }

  // ANNOTATION: Builds the full URL with query parameters and makes the HTTP call.
  // Each parameter is conditionally added using Option.fold:
  //   param.fold(uriSoFar)(uriSoFar.withQueryParam(key, _))
  // This pattern avoids if/else chains and naturally handles absent parameters.
  private def getObjects(
      offset: Option[Int],
      limit: Option[Int],
      fromDateTime: Option[ZonedDateTime],
      toDateTime: Option[ZonedDateTime],
      sort: Option[SortOrder]
  ): F[T] = {
    // ANNOTATION: emberClient.use creates an HTTP client, uses it, then closes it.
    // Resource[F, Client[F]] ensures proper lifecycle management.
    emberClient.use[T] { client =>
      // ANNOTATION: Protocol validation uses Async[F].raiseError for fail-fast behavior.
      // InvalidProtocol is a unique exception — stack trace points directly here.
      val schemeF: F[Scheme] = Scheme.fromString(protocol).fold(
        _ => Async[F].raiseError[Scheme](InvalidProtocol(protocol)),
        _.pure[F]
      )
      schemeF.flatMap { scheme =>
        val uriBase: Uri = Uri(
          scheme = Some(scheme),
          authority = Some(Uri.Authority(host = Uri.RegName(host))),
          path = Uri.Path.unsafeFromString(path)
        )
        val uriBaseWithApiKey: Uri = uriBase.withQueryParam("api_key", apiKey)
        val uriWithOffset = offset.fold(uriBaseWithApiKey)(uriBaseWithApiKey.withQueryParam("offset", _))
        val uriWithLimit = limit.fold(uriWithOffset)(uriWithOffset.withQueryParam("limit", _))
        val uriWithFromDateTime = fromDateTime.fold(uriWithLimit) { d =>
          uriWithLimit.withQueryParam("fromDateTime", d.format(Constants.OutgoingDateTimeFormat))
        }
        val uriWithToDateTime = toDateTime.fold(uriWithFromDateTime) { d =>
          uriWithFromDateTime.withQueryParam("toDateTime", d.format(Constants.OutgoingDateTimeFormat))
        }
        val uriWithSort = sort.fold(uriWithToDateTime) { sortO =>
          uriWithToDateTime.withQueryParam("sort", sortO.value)
        }

        // ANNOTATION: client.expect[T] makes the HTTP call and decodes the response
        // using the implicit EntityDecoder[F, T].
        client.expect[T](uriWithSort)(decoder)
      }
    }
  }
}

// ANNOTATION: Companion object with supporting enum.
// SortOrder uses Scala 3 enum with a factory method.
object PagingApiBase {
  enum SortOrder(val value: String) {
    case UpdateDateAsc extends SortOrder("updateDate+asc")
    case UpdateDateDesc extends SortOrder("updateDate+desc")
  }

  object SortOrder {
    def fromString(s: String): Option[SortOrder] = s match {
      case UpdateDateAsc.value  => Some(UpdateDateAsc)
      case UpdateDateDesc.value => Some(UpdateDateDesc)
      case _                    => None
    }
  }
}
```

## The Concrete Implementation

```scala
// File: gov-apis/src/main/scala/congress/gov/apis/LegislativeBillsApi.scala

package congress.gov.apis

import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.http4s.EntityDecoder
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import fs2.io.net.Network
import apiBase.PagingApiBase
import congress.gov.DTOs.LegislativeBillsDTO

// ANNOTATION: The concrete implementation is MINIMAL.
// All pagination logic is inherited from PagingApiBase.
// It only provides endpoint-specific values:
//   - protocol, host, path: where to call
//   - apiKey, pageSize: how to call
//   - emberClient: the HTTP client
//   - decoder: how to parse the response
class LegislativeBillsApi[F[_]: Async: Network](
    val protocol: String = "https",
    val host: String = "api.congress.gov",
    val apiKey: String = "DEMO_KEY",
    val path: String = "/v3/bill",
    val pageSize: Int = 250
) extends PagingApiBase[F, LegislativeBillsDTO] {
  override val emberClient: Resource[F, Client[F]] =
    EmberClientBuilder.default[F].build

  override val decoder: EntityDecoder[F, LegislativeBillsDTO] =
    LegislativeBillsDTO.entityDecoder
}

// ANNOTATION: Companion object with factory method.
// This is the pattern for creating API instances:
//   LegislativeBillsApi[IO](apiKey, pageSize)
// Returns F[ApiInstance] because construction is pure (lifted into F).
object LegislativeBillsApi {
  def apply[F[_]: Async: Network](
      apiKey: String,
      pageSize: Int
  ): F[LegislativeBillsApi[F]] = new LegislativeBillsApi[F](
    apiKey = apiKey,
    pageSize = pageSize
  ).pure[F]
}
```

## How to Create a New API Client

1. Define your response DTO extending `PagedObject` with a `Semigroup` instance
2. Create a class extending `PagingApiBase[F, YourDTO]`
3. Provide the 7 abstract members (protocol, host, apiKey, path, pageSize, decoder, emberClient)
4. Add a companion object factory method
5. The pagination, URL building, and HTTP handling are all inherited

Example for a new Votes API:
```scala
class VotesApi[F[_]: Async: Network](
    val protocol: String = "https",
    val host: String = "api.congress.gov",
    val apiKey: String = "DEMO_KEY",
    val path: String = "/v3/vote",
    val pageSize: Int = 250
) extends PagingApiBase[F, VotesDTO] {
  override val emberClient: Resource[F, Client[F]] =
    EmberClientBuilder.default[F].build
  override val decoder: EntityDecoder[F, VotesDTO] =
    VotesDTO.entityDecoder
}
```
