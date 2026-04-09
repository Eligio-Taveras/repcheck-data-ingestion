# Pattern: Test Patterns

## Pattern Summary
ScalaTest with `AnyFlatSpec` and `Matchers`, using the Fixture trait pattern for shared test data. Tests cover serialization roundtrips (encode → decode → compare), DTO conversion validation, and behavior verification. The project requires equivalence class negative testing: analyze each function line-by-line for failure points, create tests for each, and ensure unique exceptions trace back to the source.

## When To Use This Pattern
- Every module needs tests: unit, integration (local), integration (WireMock), integration (dev GCP)
- All tests run on every PR commit and must pass locally with Docker

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

// ANNOTATION: AnyFlatSpec + Matchers is the project standard.
// AnyFlatSpec gives readable "X should Y" test names.
// Matchers gives should be, should not be, etc. assertions.
class LegislativeBillSpec extends AnyFlatSpec with Matchers {

  // ANNOTATION: FIXTURE TRAIT PATTERN.
  // Define shared test data in a trait. Each test creates a new instance
  // via `new Fixture { ... }` which gives:
  //   1. Fresh data per test (no shared mutable state)
  //   2. Reusable across multiple test cases
  //   3. Easy to override specific fields in individual tests
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

    // ANNOTATION: Expected JSON string for roundtrip verification.
    // This is the canonical serialized form. If the encoder changes,
    // this test catches it immediately.
    val expectedBillJson: String =
      s"""{"congress":117,"bill_id":"hr1319-117","bill_type":"hr","latestAction":{"actionDate":"2024-04-07","actionTime":"11:14:00","text":"Passed House"},"originChamber":"House","originChamberCode":"h","title":"American Rescue Plan Act of 2021","updateDate":"2024-04-07","updateDateIncludingText":"2024-04-07T11:14:00Z[UTC]","url":"https://www.congress.gov/bill/117th-congress/house-bill/1319"}""".stripMargin
        .replace("\n", "")
        .replace("\t", "")
        .replace("\r", "")
  }

  // ANNOTATION: TEST 1 — Behavior test.
  // Verifies that the convenience method works correctly.
  "A LegislativeBill" should "correctly convert url to Uri" in new Fixture {
    bill.uri should be(
      Uri.unsafeFromString(
        "https://www.congress.gov/bill/117th-congress/house-bill/1319"
      )
    )
  }

  // ANNOTATION: TEST 2 — Serialization (encode).
  // Verifies that encoding produces the exact expected JSON.
  // .asJson uses the implicit Encoder, .noSpaces removes formatting.
  "A LegislativeBill" should "correctly serialize to JSON" in new Fixture {
    bill.asJson.noSpaces should be(expectedBillJson)
  }

  // ANNOTATION: TEST 3 — Deserialization roundtrip (decode).
  // Parses the expected JSON string back into a DTO and compares.
  // Tests the full roundtrip: object → JSON → object.
  // Uses pattern matching on the parse/decode results to give
  // clear failure messages.
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

## Equivalence Class Negative Testing (Required Pattern)

The project requires analyzing each function line-by-line for failure points and creating negative test cases using equivalence classes. Here's how to apply this:

### Step 1: Identify Failure Points

Take `LegislativeBillDTO.toDO`:
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

Failure points:
1. `BillTypes.fromString` — unknown bill type string
2. `Uri.unsafeFromString` — malformed URL

### Step 2: Define Equivalence Classes

For `BillTypes.fromString(bill_type)`:
| Class | Representative | Expected |
|-------|---------------|----------|
| Valid House bill | "HR" | Right(HouseBill) |
| Valid Senate bill | "S" | Right(SenateBill) |
| Empty string | "" | Left("Unknown bill type: ''") |
| Random invalid | "INVALID" | Left("Unknown bill type: 'INVALID'") |
| Case mismatch | "hr" | Left (case-sensitive) |
| Null-like | null | Left or exception |

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

  // Positive: valid bill type converts successfully
  "toDO" should "convert a valid HR bill type" in new Fixture {
    billWithType("HR").toDO should be a Symbol("right")
  }

  // Negative: unknown bill type returns Left with descriptive message
  "toDO" should "return Left for unknown bill type" in new Fixture {
    val result = billWithType("INVALID").toDO
    result should be a Symbol("left")
    result.left.getOrElse("") should include("Unknown bill type")
  }

  // Negative: empty string bill type returns Left
  "toDO" should "return Left for empty bill type" in new Fixture {
    billWithType("").toDO should be a Symbol("left")
  }

  // Negative: case-sensitive — lowercase "hr" should fail
  "toDO" should "return Left for lowercase bill type (case-sensitive)" in new Fixture {
    billWithType("hr").toDO should be a Symbol("left")
  }
}
```

### Step 4: If Production Code Lacks a Handler — Add One

If you discover a failure path that isn't handled (e.g., `Uri.unsafeFromString` throws on malformed URL), modify the production code:

```scala
// BEFORE: throws on malformed URL
Uri.unsafeFromString(url)

// AFTER: safe parsing with unique error
case class MalformedBillUrl(url: String)
    extends Exception(s"Malformed bill URL: '$url'")

Uri.fromString(url).left.map(_ => MalformedBillUrl(url))
```

## Database Integration Test Data — Unique Constraint Awareness

When writing integration tests that INSERT into real PostgreSQL tables, each test's data must have unique values for **all** unique constraints on the table — not just the primary key. For example, the `bills` table has both a `bill_id` primary key and a `UNIQUE (congress, bill_type, number)` constraint. Tests that vary `bill_id` but share the same `(congress, bill_type, number)` will fail with a constraint violation.

**Rule:** When creating test data helper functions (e.g., `makeBill`), expose parameters for every column involved in any unique constraint. Use distinct values per test case.

```scala
// BAD — all tests share (congress=117, bill_type=HouseBill, number=0)
private def makeBill(billId: String): LegislativeBillDO =
  LegislativeBillDO(congress = 117, billType = BillTypes.HouseBill, ...)

// GOOD — caller controls the natural key columns
private def makeBill(
  billId: String = "hr1319-117",
  congress: Int = 117,
  billType: BillTypes = BillTypes.HouseBill,
  title: String = "Default Title",
): LegislativeBillDO =
  LegislativeBillDO(congress = congress, billType = billType, ...)
```

To identify constraints, check the migration SQL in `db-migrations/src/main/resources/db/changelog/changes/`.

## Test Category Summary

| Category | Tool | Runs When | Purpose |
|----------|------|-----------|---------|
| Unit | http4s test Client, ScalaTest | Every PR commit | Logic, serialization, error paths |
| Integration (local) | DockerPostgresSpec (custom Docker CLI) | Every PR commit | SQL validation against real PostgreSQL |
| Integration (WireMock) | WireMock | Every PR commit | Failure simulation (timeouts, errors, malformed responses) |
| Integration (dev GCP) | Real dev GCP project, namespaced | Every PR commit | Contract/connection validation |

## Correlation ID in Tests

Every pipeline item gets a UUID correlation ID. Tests should verify it propagates:

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
2. Define a `Fixture` trait with representative test data
3. Write positive tests (happy path)
4. Analyze each function line-by-line for failure points
5. Define equivalence classes for each input
6. Write negative tests for each equivalence class
7. Verify each failure produces a unique, traceable exception
8. If production code lacks a handler for a discovered failure path, add one
