# Pattern: Enum with Safe Parsing

## Pattern Summary
Scala 3 enums with `Either`-based `fromString` parsing and custom Circe codecs. Parsing never throws — it returns `Either[Error, EnumValue]`. Each enum has a unique error type for traceability. Circe decoders use `.emap` to bridge the `Either` into the decoder pipeline.

## When To Use This Pattern
- Any domain type with a fixed set of values (bill types, chambers, vote results, format types)
- When external APIs send enum values as strings that need validation

## Source Files
- `gov-apis/src/main/scala/congress/gov/DTOs/BillTypes.scala` — enum with Either-based parsing
- `gov-apis/src/main/scala/congress/gov/DTOs/Internal/FormatType.scala` — enum with Circe codecs and unique error type

---

## Pattern A: Enum with Either-Based Parsing (BillTypes)

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/BillTypes.scala

package congress.gov.DTOs

// ANNOTATION: Scala 3 enum with a value parameter.
// Each variant carries a string value (the Congress.gov API representation).
// billType.toString gives the variant name, billType.value gives the API string.
enum BillTypes(val value: String) {
  case HouseBill extends BillTypes("h.r")
  case SimpleHouseResolution extends BillTypes("h.res")
  case ConcurrentHouseResolution extends BillTypes("h.con.res")
  case JointResolutionOriginHouse extends BillTypes("h.j.res")
  case SenateBill extends BillTypes("s")
  case SimpleSenateResolution extends BillTypes("s.res")
  case ConcurrentSenateResolution extends BillTypes("s.con.res")
  case JoinResolutionOriginSenate extends BillTypes("s.j.res")
  case PublicLaw extends BillTypes("p.l.")
  case StatutesAtLarge extends BillTypes("stat")
  case USCode extends BillTypes("u.s.c")
  case SenateReport extends BillTypes("s.rpt")
  case HouseReport extends BillTypes("h.rpt")
}

object BillTypes {
  // ANNOTATION: SAFE PARSING — returns Either, never throws.
  //
  // WHY EITHER AND NOT OPTION?
  //   Either[String, BillTypes] carries the error message.
  //   Option would lose the information about WHAT was unrecognized.
  //   The caller (LegislativeBillDTO.toDO) propagates this Either
  //   all the way up to the stream processing level.
  //
  // WHY NOT A MAP LOOKUP?
  //   Explicit match gives compile-time exhaustiveness checking
  //   if you add a new case to the enum later. A Map would silently
  //   miss new variants.
  //
  // NOTE: The match values ("HR", "S", etc.) are the Congress.gov API
  // abbreviations, which differ from the enum's .value strings.
  def fromString(value: String): Either[String, BillTypes] =
    value match {
      case "HR"      => Right(BillTypes.HouseBill)
      case "HRES"    => Right(BillTypes.SimpleHouseResolution)
      case "HCONRES" => Right(BillTypes.ConcurrentHouseResolution)
      case "HJRES"   => Right(BillTypes.JointResolutionOriginHouse)
      case "S"       => Right(BillTypes.SenateBill)
      case "SRES"    => Right(BillTypes.SimpleSenateResolution)
      case "SCONRES" => Right(BillTypes.ConcurrentSenateResolution)
      case "SJRES"   => Right(BillTypes.JoinResolutionOriginSenate)
      case "PL"      => Right(BillTypes.PublicLaw)
      case "STAT"    => Right(BillTypes.StatutesAtLarge)
      case "USC"     => Right(BillTypes.USCode)
      case "SRPT"    => Right(BillTypes.SenateReport)
      case "HRPT"    => Right(BillTypes.HouseReport)
      case other     => Left(s"Unknown bill type: '$other'")
    }
}
```

## Pattern B: Enum with Unique Error Type and Circe Codecs (FormatType)

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/Internal/FormatType.scala

package congress.gov.DTOs.Internal

import io.circe.{Decoder, Encoder}

// ANNOTATION: UNIQUE ERROR TYPE per enum.
// This is the project pattern: every exceptional case gets its own
// exception class. When UnrecognizedFormatType appears in logs,
// you know EXACTLY what failed and where.
//
// Extends Exception so it can be thrown/raised in the effect system.
// The message includes the unrecognized value for debugging.
case class UnrecognizedFormatType(text: String)
    extends Exception(s"Unrecognized FormatType: '$text'")

enum FormatType(val text: String) {
  case formattedText extends FormatType("Formatted Text")
  case pdf extends FormatType("PDF")
  case formattedXml extends FormatType("Formatted XML")
}

object FormatType {
  // ANNOTATION: Same Either pattern as BillTypes, but returns
  // the unique error type instead of a raw String.
  // This gives you type-safe error handling downstream:
  //   .left.map(_.getMessage) for Circe
  //   .left.map(Async[F].raiseError(_)) for effect-level errors
  def fromString(text: String): Either[UnrecognizedFormatType, FormatType] =
    text match {
      case "Formatted Text" => Right(FormatType.formattedText)
      case "PDF"            => Right(FormatType.pdf)
      case "Formatted XML"  => Right(FormatType.formattedXml)
      case other            => Left(UnrecognizedFormatType(other))
    }

  // ANNOTATION: Circe ENCODER — converts FormatType → JSON string.
  // contramap transforms the input: FormatType → String → JSON.
  implicit val encoder: Encoder[FormatType] =
    Encoder.encodeString.contramap(_.text)

  // ANNOTATION: Circe DECODER — converts JSON string → FormatType.
  // Uses .emap (Either-map) to bridge fromString's Either into the
  // Circe decoder pipeline. If fromString returns Left, the decoder
  // fails with that error message.
  //
  // This is THE STANDARD PATTERN for enum decoding with Circe:
  //   1. Decode the raw string
  //   2. emap to validate/convert via Either
  //   3. .left.map to convert error type to String (Circe requirement)
  implicit val decoder: Decoder[FormatType] = Decoder.decodeString.emap { str =>
    fromString(str).left.map(_.getMessage)
  }
}
```

## The Two Approaches Compared

| Aspect | BillTypes | FormatType |
|--------|-----------|------------|
| Error type | `String` | Unique `UnrecognizedFormatType` exception |
| Circe codecs | None (uses GovSite manual decoder) | `encoder`/`decoder` via emap |
| Where parsing happens | In DTO.toDO conversion | In Circe decoder (during JSON parsing) |
| Best for | Enums parsed during DTO→DO conversion | Enums embedded in JSON that need auto-decoding |

## How to Create a New Enum

For new enums, prefer the FormatType pattern (unique error + Circe codecs):

```scala
case class UnrecognizedChamber(value: String)
    extends Exception(s"Unrecognized chamber: '$value'")

enum Chamber(val apiValue: String) {
  case House extends Chamber("House")
  case Senate extends Chamber("Senate")
}

object Chamber {
  def fromString(value: String): Either[UnrecognizedChamber, Chamber] =
    value match {
      case "House"  => Right(Chamber.House)
      case "Senate" => Right(Chamber.Senate)
      case other    => Left(UnrecognizedChamber(other))
    }

  implicit val encoder: Encoder[Chamber] =
    Encoder.encodeString.contramap(_.apiValue)
  implicit val decoder: Decoder[Chamber] = Decoder.decodeString.emap { str =>
    fromString(str).left.map(_.getMessage)
  }
}
```
