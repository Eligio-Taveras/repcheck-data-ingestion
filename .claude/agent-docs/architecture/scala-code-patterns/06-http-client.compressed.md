<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/scala-code-patterns/06-http-client.md -->

# HTTP Client (http4s)

**Pattern**: http4s Ember client with `Resource` lifecycle management.

```scala
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.client.Client
import cats.effect.Resource

val emberClient: Resource[F, Client[F]] = EmberClientBuilder.default[F].build

def fetchData[F[_]: Async: Network](uri: Uri): F[String] =
  emberClient.use { client =>
    client.expect[String](uri)
  }
```

## URI Construction

```scala
import org.http4s.Uri

val baseUri = Uri(
  scheme = Some(Uri.Scheme.https),
  authority = Some(Uri.Authority(host = Uri.RegName("api.congress.gov"))),
  path = Uri.Path.unsafeFromString("/v3/bill")
)

val requestUri = baseUri
  .withQueryParam("api_key", apiKey)
  .withQueryParam("limit", pageSize)
  .withQueryParam("offset", offset)
  .withQueryParam("sort", "updateDate+asc")
  .withOptionalQueryParam("fromDateTime", fromDateTime.map(_.format(formatter)))
```

## Rules

- Always use `Resource` for client lifecycle — never construct clients manually
- Use `.use { client => ... }` to ensure cleanup
- Use `withOptionalQueryParam` for optional parameters — no null checks
- Use Circe `EntityDecoder` integration for typed response parsing