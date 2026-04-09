<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/test-patterns.md -->

# Pattern: Test Patterns

## When To Use
- Every module: unit, integration (local), integration (WireMock), integration (dev GCP)
- All tests run on every PR commit, must pass locally with Docker

## Source Files
- `gov-apis/src/test/scala/apiBase/congress/gov/DTOs/LegislativeBillSpec.scala` — serialization roundtrip tests

---

## The Existing Test

```scala
// File: gov-apis/src/test/scala/apiBase/congress/gov/DTOs/LegislativeBillSpec.scala

package apiBase.congress.gov.DTOs

import java.time.{ZoneId, ZonedDateTime}

import io.circe.parser.parse
import io.circe.syntax._
import io.circe.{Json, ParsingFailure}

import org.http4s.Uri

import congress.gov.DTOs.Internal.LegislativeBillDTO._
import congress.gov.DTOs.{LatestAction, LegislativeBillDTO}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LegislativeBillSpec extends AnyFlatSpec with Matchers {

  // AnyFlatSpec + Matchers is project standard. Fixture trait pattern: fresh data per test, reusable, easy field override.
  trait Fixture {
    val timeOfBill: ZonedDateTime =
      ZonedDateTime.of(2024, 4, 7, 11, 14, 0, 0, ZoneId.of("UTC"))
    val bill: LegislativeBillDTO = LegislativeBillDTO(
      congress = 117,
      bill_id = "hr1319-117",
      bill_type = "hr",
      latestAction = LatestAction(timeOfBill, "Passed House"),
      originChamber = "House",
      originChamberCode = "h",
      title = "American Rescue Plan Act of 2021",
      updateDate = timeOfBill.toLocalDate,
      updateDateIncludingText = timeOfBill,
      url = "https://www.congress.gov/bill/117th-congress/house-bill/1319"
    )

    // Canonical serialized form for roundtrip verification
    val expectedBillJson: String =
      s"""{"congress":117,"bill_id":"hr1319-117","bill_type":"hr","latestAction":{"actionDate":"2024-04-07","actionTime":"11:14:00","text":"Passed House"},"originChamber":"House","originChamberCode":"h","title":"American Rescue Plan Act of 2021","updateDate":"2024-04-07","updateDateIncludingText":"2024-04-07T11:14:00Z[UTC]","url":"https://www.congress.gov/bill/117th-congress/house-bill/1319"}""".stripMargin
        .replace("\n", "")
        .replace("\t", "")
        .replace("\r", "")
  }

  // Behavior: convenience method url → Uri
  "A LegislativeBill" should "correctly convert url to Uri" in new Fixture {
    bill.uri should be(
      Uri.unsafeFromString(
        "https://www.congress.gov/bill/117th-congress/house-bill/1319"
      )
    )
  }

  // Serialization: object → JSON
  "A LegislativeBill" should "correctly serialize to JSON" in new Fixture {
    bill.asJson.noSpaces should be(expectedBillJson)
  }

  // Deserialization roundtrip: JSON → object
  "A LegislativeBill" should "correctly deserialize from JSON" in new Fixture {
    parse(expectedBillJson) match
      case Left(error: ParsingFailure) => fail(error)
      case Right(json: Json) => {
        json.as[LegislativeBillDTO] match
          case Left(error) => fail(error)
          case Right(decodedBill) => {
            decodedBill should be(bill)
          }
      }
  }
}
```

## Equivalence Class Negative Testing (Required)

Analyze each function line-by-line for failure points, create tests for each, ensure unique exceptions trace back to source.

### Step 1: Identify Failure Points

Example: `LegislativeBillDTO.toDO`:
```scala
def toDO: Either[String, LegislativeBillDO] =
  BillTypes.fromString(bill_type).map { billType =>  // ← Can fail: unknown bill_type
    LegislativeBillDO(
      congress,
      bill_id,
      billType,
      latestAction,
      originChamber,
      originChamberCode,
      title,
      ZonedDateTime.of(LocalDateTime.of(updateDate, LocalTime.of(0, 0)), ZoneId.of("UTC")),
      updateDateIncludingText,
      Uri.unsafeFromString(url)   // ← Can fail: malformed URL
    )
  }
```

Failure points: (1) `BillTypes.fromString` — unknown type, (2) `Uri.unsafeFromString` — malformed URL

### Step 2: Define Equivalence Classes

| Class | Representative | Expected |
|-------|---------------|----------|
| Valid House bill | "HR" | Right(HouseBill) |
| Valid Senate bill | "S" | Right(SenateBill) |
| Empty string | "" | Left("Unknown bill type: ''") |
| Random invalid | "INVALID" | Left("Unknown bill type: 'INVALID'") |
| Case mismatch | "hr" | Left (case-sensitive) |

### Step 3: Write Negative Tests

```scala
class LegislativeBillDTOSpec extends AnyFlatSpec with Matchers {
  trait Fixture {
    def billWithType(billType: String): LegislativeBillDTO =
      LegislativeBillDTO(
        congress = 117, bill_id = "hr1-117", bill_type = billType,
        latestAction = LatestAction(ZonedDateTime.now(), "test"),
        originChamber = "House", originChamberCode = "h",
        title = "Test", updateDate = LocalDate.now(),
        updateDateIncludingText = ZonedDateTime.now(),
        url = "https://congress.gov/bill/117/hr/1"
      )
  }

  "toDO" should "convert a valid HR bill type" in new Fixture {
    billWithType("HR").toDO should be a Symbol("right")
  }

  "toDO" should "return Left for unknown bill type" in new Fixture {
    val result = billWithType("INVALID").toDO
    result should be a Symbol("left")
    result.left.getOrElse("") should include("Unknown bill type")
  }

  "toDO" should "return Left for empty bill type" in new Fixture {
    billWithType("").toDO should be a Symbol("left")
  }

  "toDO" should "return Left for lowercase bill type (case-sensitive)" in new Fixture {
    billWithType("hr").toDO should be a Symbol("left")
  }
}
```

### Step 4: Add Production Handler If Missing

If production code lacks handler for discovered failure path:

```scala
// BEFORE: throws on malformed URL
Uri.unsafeFromString(url)

// AFTER: safe parsing with unique error
case class MalformedBillUrl(url: String)
    extends Exception(s"Malformed bill URL: '$url'")

Uri.fromString(url).left.map(_ => MalformedBillUrl(url))
```

## Database Integration Test Data — Unique Constraint Awareness

Each test's data must have unique values for **all** unique constraints, not just primary key. Example: `bills` table has `bill_id` PK and `UNIQUE (congress, bill_type, number)` constraint. Tests varying `bill_id` but sharing `(congress, bill_type, number)` fail with constraint violation.

Expose parameters for every column in any unique constraint:

```scala
// BAD — all tests share (congress=117, bill_type=HouseBill, number=0)
private def makeBill(billId: String): LegislativeBillDO =
  LegislativeBillDO(congress = 117, billType = BillTypes.HouseBill, ...)

// GOOD — caller controls natural key columns
private def makeBill(
  billId: String = "hr1319-117",
  congress: Int = 117,
  billType: BillTypes = BillTypes.HouseBill,
  title: String = "Default Title",
): LegislativeBillDO =
  LegislativeBillDO(congress = congress, billType = billType, ...)
```

Check migration SQL in `db-migrations/src/main/resources/db/changelog/changes/` to identify constraints.

## Test Category Summary

| Category | Tool | Runs When | Purpose |
|----------|------|-----------|---------|
| Unit | http4s test Client, ScalaTest | Every PR commit | Logic, serialization, error paths |
| Integration (local) | DockerPostgresSpec (custom Docker CLI) | Every PR commit | SQL validation against real PostgreSQL |
| Integration (WireMock) | WireMock | Every PR commit | Failure simulation (timeouts, errors, malformed responses) |
| Integration (dev GCP) | Real dev GCP project, namespaced | Every PR commit | Contract/connection validation |

## Correlation ID in Tests

Every pipeline item gets a UUID correlation ID. Tests should verify propagation:

```scala
"pipeline" should "assign correlation ID to ProcessingResult" in {
  val result = pipeline.processItem(testBill)
  result.correlationId should not be empty
}

"pipeline" should "include correlation ID in error messages" in {
  val result = pipeline.processItem(invalidBill)
  result.errorMessage.get should include(result.correlationId)
}
```

## How to Create Tests for a New Module

1. Create spec class extending `AnyFlatSpec with Matchers`
2. Define `Fixture` trait with representative test data
3. Write positive tests (happy path)
4. Analyze each function line-by-line for failure points
5. Define equivalence classes for each input
6. Write negative tests for each equivalence class
7. Verify each failure produces unique, traceable exception
8. If production code lacks handler for discovered failure path, add one