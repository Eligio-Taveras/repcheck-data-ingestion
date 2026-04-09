> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 17. Testing

**Pattern**: ScalaTest with `AnyFlatSpec with Matchers`. Fixture trait for shared test setup.

### Unit Test

```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LegislativeBillApiDTOSpec extends AnyFlatSpec with Matchers {

  trait Fixture {
    val sampleJson = """{"congress": 118, "number": "1234", ...}"""
    val expectedBillId = "hr-1234-118"
  }

  "LegislativeBillApiDTO" should "decode from Congress.gov JSON" in new Fixture {
    val result = io.circe.parser.decode[LegislativeBillApiDTO](sampleJson)
    result shouldBe a[Right[_, _]]
    result.toOption.get.congress shouldBe 118
  }

  it should "convert to domain object" in new Fixture {
    val dto = io.circe.parser.decode[LegislativeBillApiDTO](sampleJson).toOption.get
    val bill = dto.toDO()
    bill.billId shouldBe expectedBillId
    bill.billType shouldBe BillType.HouseBill
  }
}
```

### IO Test (for effectful code)

```scala
import cats.effect.IO
import cats.effect.unsafe.implicits.global

class AlloyDbRepositorySpec extends AnyFlatSpec with Matchers with ForAllTestContainer {

  override val container = PostgreSQLContainer()

  lazy val xa = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = container.jdbcUrl,
    user = container.username,
    password = container.password
  )

  "AlloyDbRepository" should "save and retrieve a bill" in {
    val result = (for {
      _    <- upsertBill(testBill).transact(xa)
      bill <- findBill(testBill.billId).transact(xa)
    } yield bill).unsafeRunSync()

    result shouldBe defined
  }
}
```

### Rules
- Use `AnyFlatSpec with Matchers` for all tests
- Use `trait Fixture` for shared test data
- IO tests use `.unsafeRunSync()` — this is the **only** acceptable use of `unsafeRunSync`
- Test DTOs: verify JSON decoding, DTO → DO conversion, and DO → DTO conversion
- Test errors: verify specific exception types are thrown with correct messages
