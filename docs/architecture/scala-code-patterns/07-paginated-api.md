> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 7. Paginated API Base

**Pattern**: Generic trait for paginated Congress.gov API calls with FS2 streaming and Semigroup-based batch combining.

```scala
import cats.Semigroup
import cats.effect.Async
import org.http4s.client.Client
import org.http4s.EntityDecoder

trait PagedObject {
  def lengthRetrieved: Int
}

trait PagingApiBase[F[_]: Async: Network, T <: PagedObject: Semigroup] {
  protected def protocol: String
  protected def host: String
  protected def apiKey: String
  protected def path: String
  protected def pageSize: Int
  implicit protected def decoder: EntityDecoder[F, T]
  protected def emberClient: Resource[F, Client[F]]

  enum SortOrder(val value: String) {
    case UpdateDateAsc extends SortOrder("updateDate+asc")
    case UpdateDateDesc extends SortOrder("updateDate+desc")
  }

  // Stream a single batch (one page)
  def streamBatch(
    fromDateTime: Option[ZonedDateTime] = None,
    toDateTime: Option[ZonedDateTime] = None,
    offset: Int = 0,
    sortOrder: Option[SortOrder] = Some(SortOrder.UpdateDateAsc)
  ): fs2.Stream[F, T] =
    fs2.Stream.eval(getObjects(Some(offset), Some(pageSize), fromDateTime, toDateTime, sortOrder))

  // Fetch all pages recursively, combining via Semigroup
  def getAll(
    offset: Int = 0,
    fromDateTime: Option[ZonedDateTime] = None,
    toDateTime: Option[ZonedDateTime] = None,
    sortOrder: Option[SortOrder] = Some(SortOrder.UpdateDateAsc)
  ): F[T]

  // Fetch a single page
  private def getObjects(
    offset: Option[Int],
    limit: Option[Int],
    fromDateTime: Option[ZonedDateTime],
    toDateTime: Option[ZonedDateTime],
    sortOrder: Option[SortOrder]
  ): F[T]
}
```

### Concrete Implementation

```scala
class LegislativeBillsApi[F[_]: Async: Network](
  override protected val apiKey: String,
  override protected val pageSize: Int
) extends PagingApiBase[F, LegislativeBillsApiDTO] {

  override protected val protocol = "https"
  override protected val host = "api.congress.gov"
  override protected val path = "/v3/bill"

  override implicit protected val decoder: EntityDecoder[F, LegislativeBillsApiDTO] =
    LegislativeBillsApiDTO.entityDecoder

  override protected val emberClient: Resource[F, Client[F]] =
    EmberClientBuilder.default[F].build
}
```

### Rules
- All Congress.gov API clients extend `PagingApiBase`
- The `T` type must have a `Semigroup` for combining pages (`|+|`)
- The `T` type must extend `PagedObject` so pagination knows when to stop
- Stop recursion when `lengthRetrieved < pageSize`
