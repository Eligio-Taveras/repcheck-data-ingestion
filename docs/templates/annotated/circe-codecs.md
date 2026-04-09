# Pattern: Circe Codecs

## Pattern Summary
Three approaches to JSON serialization with Circe, each used for a different purpose: semi-auto derivation for internal DTOs, manual decoders for external APIs with mismatched field names, and custom codecs for domain types (dates, enums). The choice depends on whether field names match and whether you need custom serialization logic.

## When To Use This Pattern
- Any DTO that needs JSON serialization/deserialization
- Custom types (dates, enums, URIs) that aren't natively supported by Circe
- Bridging external API field names to internal naming conventions

## Source Files
- `gov-apis/src/main/scala/congress/gov/DTOs/Internal/LegislativeBillDTO.scala` — semi-auto derivation
- `gov-apis/src/main/scala/congress/gov/DTOs/GovSite/LegislativeBillDTOGovSite.scala` — manual decoder
- `gov-apis/src/main/scala/common/Serializers.scala` — custom ZonedDateTime codecs
- `gov-apis/src/main/scala/congress/gov/DTOs/LatestAction.scala` — custom encoder splitting fields
- `gov-apis/src/main/scala/congress/gov/DTOs/Internal/FormatType.scala` — enum emap pattern

---

## Approach 1: Semi-Auto Derivation (Internal DTOs)

Use when field names in JSON match case class field names exactly.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/Internal/LegislativeBillDTO.scala

package congress.gov.DTOs.Internal

import cats.effect.Concurrent

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

import org.http4s.EntityDecoder

import congress.gov.DTOs.LegislativeBillDTO

object LegislativeBillDTO {
  // ANNOTATION: Semi-auto derivation generates encoder/decoder from case class fields.
  // Field names in JSON must EXACTLY match case class field names.
  // Semi-auto (not full auto) means you explicitly declare the instances —
  // this is preferred because it makes the implicit scope explicit and
  // avoids unexpected derivation of types you didn't intend.
  implicit val decoder: Decoder[LegislativeBillDTO] =
    deriveDecoder[LegislativeBillDTO]
  implicit val encoder: Encoder[LegislativeBillDTO] =
    deriveEncoder[LegislativeBillDTO]

  // ANNOTATION: http4s bridge — creates an EntityDecoder from the Circe Decoder.
  // This lets http4s client.expect[T](uri) automatically parse JSON responses.
  // The Concurrent constraint is needed by http4s for streaming parsing.
  implicit def entityDecoder[F[_]: Concurrent]
  : EntityDecoder[F, LegislativeBillDTO] =
    org.http4s.circe.jsonOf[F, LegislativeBillDTO]
}
```

## Approach 2: Manual Decoder (External API Field Mapping)

Use when external API field names don't match your case class.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/GovSite/LegislativeBillDTOGovSite.scala

package congress.gov.DTOs.GovSite

import java.time.{LocalDate, ZonedDateTime}

import io.circe.Decoder

import congress.gov.DTOs.{LatestAction, LegislativeBillDTO}

object LegislativeBillDTOGovSite {

  import common.Serializers.*

  // ANNOTATION: Manual HCursor decoder for field name mapping.
  //
  // The Congress.gov API returns:    Our DTO expects:
  //   "number"                        bill_id
  //   "type"                          bill_type
  //
  // c.downField("number").as[String] navigates to the "number" field
  // and decodes it as a String, but we yield it as bill_id.
  //
  // The for-comprehension works because Decoder.Result is an Either —
  // if ANY field fails to decode, the whole decoder fails with a
  // descriptive error showing which field and why.
  implicit val govSiteDecoder: Decoder[LegislativeBillDTO] =
    (c: io.circe.HCursor) =>
      for {
        congress <- c.downField("congress").as[Int]
        bill_id <- c.downField("number").as[String]     // MAPPING: "number" → bill_id
        bill_type <- c.downField("type").as[String]      // MAPPING: "type" → bill_type
        latestAction <- c.downField("latestAction").as[LatestAction]
        originChamber <- c.downField("originChamber").as[String]
        originChamberCode <- c.downField("originChamberCode").as[String]
        title <- c.downField("title").as[String]
        updateDate <- c.downField("updateDate").as[LocalDate]
        updateDateIncludingText <- c
          .downField("updateDateIncludingText")
          .as[ZonedDateTime]                              // Uses custom decoder from Serializers
        url <- c.downField("url").as[String]
      } yield LegislativeBillDTO(
        congress, bill_id, bill_type, latestAction,
        originChamber, originChamberCode, title,
        updateDate, updateDateIncludingText, url
      )
}
```

## Approach 3: Custom Codecs for Domain Types

Use for types that aren't natively supported (dates, enums, URIs).

```scala
// File: gov-apis/src/main/scala/common/Serializers.scala

package common

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.circe.{Decoder, Encoder}

object Serializers {
  // ANNOTATION: Custom ZonedDateTime decoder.
  // Circe doesn't have a built-in ZonedDateTime decoder because
  // date formats vary across APIs. We define our own using ISO format.
  //
  // PATTERN: Decoder.decodeString.emap
  //   1. Decode the JSON value as a raw String
  //   2. .emap transforms the String → Either[String, T]
  //   3. If parsing fails, the Left message becomes the decoder error
  //
  // scala.util.Try wraps the Java parsing exception,
  // .toEither converts it, .left.map extracts the error message.
  implicit val zonedDateTimeDecoder: Decoder[ZonedDateTime] =
    Decoder.decodeString.emap { str =>
      scala.util
        .Try {
          ZonedDateTime.parse(str, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }
        .toEither
        .left
        .map(error => {
          "Failed to decode datetime: " + error.getMessage
        })
    }

  // ANNOTATION: Custom ZonedDateTime encoder.
  // Encoder.encodeString.contramap works in the opposite direction:
  //   1. Start with a String encoder
  //   2. contramap transforms the input: ZonedDateTime → String
  //   3. The String encoder handles the rest
  implicit val zonedDateTimeEncoder: Encoder[ZonedDateTime] =
    Encoder.encodeString.contramap[ZonedDateTime](dt =>
      val strVal = dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)
      strVal
    )
}
```

## Approach 4: Custom Encoder Reshaping Fields

Use when the serialized JSON shape differs from the case class shape.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/LatestAction.scala

package congress.gov.DTOs

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}
import scala.jdk.CollectionConverters._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, HCursor, Json}

case class LatestAction(actionDate: ZonedDateTime, text: String)

object LatestAction {
  // ANNOTATION: Custom encoder that SPLITS a single field into two.
  // The case class has one ZonedDateTime, but the JSON has separate
  // "actionDate" (LocalDate) and "actionTime" (LocalTime) fields.
  // This matches the Congress.gov API format.
  //
  // Json.obj creates a JSON object from tuples.
  // .asJson uses the Circe encoder for each value type.
  implicit val encoder: Encoder[LatestAction] = (a: LatestAction) => {
    Json.obj(
      ("actionDate", a.actionDate.toLocalDate.asJson),
      ("actionTime", a.actionDate.toLocalTime.asJson),
      ("text", Json.fromString(a.text))
    )
  }

  // ANNOTATION: Custom decoder that COMBINES two fields into one.
  // Reads "actionDate" and optional "actionTime", combines into ZonedDateTime.
  // "actionTime" is Optional because some API responses omit it.
  // .fold provides a default midnight time if absent.
  implicit val decoder: Decoder[LatestAction] = (c: HCursor) => {
    for {
      actionLocalDate <- c.downField("actionDate").as[LocalDate]
      actionLocalTime <- c.downField("actionTime").as[Option[LocalTime]]
      text <- c.downField("text").as[String]
    } yield {
      val actionTime = actionLocalTime.fold(LocalTime.of(0, 0, 0))(identity)
      val actionDate =
        ZonedDateTime.of(actionLocalDate, actionTime, ZoneId.of("UTC"))
      LatestAction(actionDate, text)
    }
  }
}
```

## Decision Guide

| Situation | Approach | Import |
|-----------|----------|--------|
| Field names match exactly | Semi-auto derivation | `io.circe.generic.semiauto._` |
| External API field names differ | Manual HCursor decoder | `io.circe.Decoder` |
| Custom types (dates, enums) | `emap` / `contramap` | `io.circe.{Decoder, Encoder}` |
| JSON shape differs from case class | Manual `Json.obj` encoder | `io.circe.{Encoder, Json}` |
| http4s integration | `jsonOf[F, T]` EntityDecoder | `org.http4s.circe.jsonOf` |

## How to Create Codecs for a New DTO

```scala
// For a new VoteDTO with matching field names:
object VoteDTO {
  implicit val decoder: Decoder[VoteDTO] = deriveDecoder[VoteDTO]
  implicit val encoder: Encoder[VoteDTO] = deriveEncoder[VoteDTO]
  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, VoteDTO] =
    org.http4s.circe.jsonOf[F, VoteDTO]
}

// For a GovSite decoder with different field names:
object VoteDTOGovSite {
  implicit val decoder: Decoder[VoteDTO] = (c: HCursor) =>
    for {
      voteId    <- c.downField("roll_call").as[String]   // API: roll_call → voteId
      chamber   <- c.downField("chamber").as[String]
      result    <- c.downField("result").as[String]
    } yield VoteDTO(voteId, chamber, result)
}
```
