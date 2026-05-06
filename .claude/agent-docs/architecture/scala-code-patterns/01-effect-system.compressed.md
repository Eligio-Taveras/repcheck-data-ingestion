<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/scala-code-patterns/01-effect-system.md -->

# Effect System

**Pattern**: Tagless final everywhere using Cats Effect type classes.

All code is polymorphic in the effect type `F[_]`. Libraries use the minimum required constraint. Application entry points wire in `IO`.

```scala
// Library code — use the minimum constraint needed
trait BillRepository[F[_]: Sync] {
  def findById(billId: String): F[Option[LegislativeBillDO]]
  def save(bill: LegislativeBillDO): F[Unit]
}

// Code that needs async/concurrent capabilities
trait LlmAdapter[F[_]: Async] {
  def submit(request: LlmRequest): F[LlmResponse]
}

// Code that needs networking
class CongressApiClient[F[_]: Async: Network] extends PagingApiBase[F, LegislativeBillsDTO]

// Application entry point — wires in concrete IO
object BillIngestionApp extends IOApp.Simple {
  def run: IO[Unit] = {
    val repo = new AlloyDbBillRepository[IO](xa)
    val client = new CongressApiClient[IO](config)
    // ...
  }
}
```

### Constraint Hierarchy (use the weakest that works)

| Constraint | Use When |
|---|---|
| `Applicative[F]` | Pure lifting, no sequencing needed |
| `Monad[F]` | Sequencing, flatMap chains |
| `Sync[F]` | Blocking I/O (GCS SDK, JDBC) |
| `Async[F]` | Non-blocking I/O, concurrency, sleep/timeout |
| `Async[F]: Network` | HTTP client (http4s) |

### Rules
- Never use `IO` directly in library code — always `F[_]` with constraints
- Never use `unsafeRunSync()` except in test helpers
- Application entry points extend `IOApp.Simple` and wire `IO`