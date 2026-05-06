<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/enum-with-parsing.md -->

# Pattern: Enum with Safe Parsing

Scala 3 enums with `Either`-based `fromString` parsing and custom Circe codecs. Parsing never throws — returns `Either[Error, EnumValue]`. Each enum has a unique error type for traceability. Circe decoders use `.emap` to bridge the `Either` into decoder pipeline.

## When To Use
- Fixed set of domain values (bill types, chambers, vote results, format types)
- External APIs send enum values as strings requiring validation

---

## Pattern A: Enum with Either-Based Parsing (BillTypes)

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/BillTypes.scala

package congress.gov.DTOs

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
  // SAFE PARSING — returns Either, never throws. Explicit match ensures compile-time exhaustiveness.
  // Match values are Congress.gov API abbreviations (differ from enum .value strings).
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

---

## Pattern B: Enum with Unique Error Type and Circe Codecs (FormatType)

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/Internal/FormatType.scala

package congress.gov.DTOs.Internal

import io.circe.{Decoder, Encoder}

// Unique error type per enum — provides type-safe error handling and clear logging.
case class UnrecognizedFormatType(text: String)
    extends Exception(s"Unrecognized FormatType: '$text'")

enum FormatType(val text: String) {
  case formattedText extends FormatType("Formatted Text")
  case pdf extends FormatType("PDF")
  case formattedXml extends FormatType("Formatted XML")
}

object FormatType {
  // Returns unique error type for type-safe downstream handling.
  def fromString(text: String): Either[UnrecognizedFormatType, FormatType] =
    text match {
      case "Formatted Text" => Right(FormatType.formattedText)
      case "PDF"            => Right(FormatType.pdf)
      case "Formatted XML"  => Right(FormatType.formattedXml)
      case other            => Left(UnrecognizedFormatType(other))
    }

  // Encodes FormatType → JSON string via .text field.
  implicit val encoder: Encoder[FormatType] =
    Encoder.encodeString.contramap(_.text)

  // Decodes JSON string → FormatType. Uses .emap to bridge Either into Circe pipeline.
  // If fromString returns Left, decoder fails with error message. Standard pattern for enum decoding.
  implicit val decoder: Decoder[FormatType] = Decoder.decodeString.emap { str =>
    fromString(str).left.map(_.getMessage)
  }
}
```

---

## Approaches Compared

| Aspect | BillTypes | FormatType |
|--------|-----------|------------|
| Error type | `String` | Unique `UnrecognizedFormatType` exception |
| Circe codecs | None (DTO.toDO conversion) | `encoder`/`decoder` via emap |
| Parsing location | DTO→DO conversion | Circe decoder (JSON parsing) |
| Use case | Enums during DTO conversion | Enums embedded in JSON needing auto-decoding |

---

## How to Create a New Enum

Prefer FormatType pattern (unique error + Circe codecs):

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