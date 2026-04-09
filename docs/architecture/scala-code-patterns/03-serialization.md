> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 3. Serialization (Circe)

**Pattern**: Semi-automatic derivation as the default. Custom encoders/decoders only when field mapping differs from the case class.

### Default: Semi-Auto Derivation

```scala
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class TextVersion(date: LocalDateTime, formats: List[Format])

object TextVersion {
  implicit val decoder: Decoder[TextVersion] = deriveDecoder[TextVersion]
  implicit val encoder: Encoder[TextVersion] = deriveEncoder[TextVersion]
}
```

### Custom Decoders (when API field names differ)

```scala
object LegislativeBillApiDTO {
  implicit val decoder: Decoder[LegislativeBillApiDTO] = (c: HCursor) =>
    for {
      congress       <- c.downField("congress").as[Int]
      number         <- c.downField("number").as[String]
      billType       <- c.downField("type").as[String]
      title          <- c.downField("title").as[String]
      originChamber  <- c.downField("originChamber").as[String]
      latestAction   <- c.downField("latestAction").as[LatestActionApiDTO]
      updateDate     <- c.downField("updateDate").as[String]
      url            <- c.downField("url").as[String]
    } yield LegislativeBillApiDTO(
      congress, number, billType, title,
      originChamber, latestAction, updateDate, url
    )
}
```

### Enum Serialization

```scala
enum BillType(val value: String) {
  case HouseBill extends BillType("hr")
  case SenateBill extends BillType("s")
  case HouseResolution extends BillType("hres")
  // ...
}

object BillType {
  def fromString(s: String): BillType =
    BillType.values.find(_.value.equalsIgnoreCase(s))
      .getOrElse(throw UnknownBillType(s))

  implicit val decoder: Decoder[BillType] =
    Decoder.decodeString.emapTry(str => scala.util.Try(fromString(str)))

  implicit val encoder: Encoder[BillType] =
    Encoder.encodeString.contramap(_.value)
}
```

### http4s EntityDecoder Integration

```scala
// Enable http4s to decode HTTP responses directly into typed objects
object LegislativeBillsApiDTO {
  implicit val circeDecoder: Decoder[LegislativeBillsApiDTO] = deriveDecoder
  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, LegislativeBillsApiDTO] =
    org.http4s.circe.jsonOf[F, LegislativeBillsApiDTO]
}
```

### Semigroup for Batch Combining

```scala
import cats.Semigroup

// Used by PagingApiBase to combine paginated results
implicit val semigroup: Semigroup[LegislativeBillsApiDTO] =
  (x, y) => LegislativeBillsApiDTO(x.bills ++ y.bills)
```

### Shared Serializers

Defined once in `repcheck-shared-models`:

```scala
object Serializers {
  implicit val zonedDateTimeDecoder: Decoder[ZonedDateTime] =
    Decoder.decodeString.emapTry { str =>
      scala.util.Try(ZonedDateTime.parse(str, DateTimeFormatter.ISO_ZONED_DATE_TIME))
    }

  implicit val zonedDateTimeEncoder: Encoder[ZonedDateTime] =
    Encoder.encodeString.contramap(_.format(DateTimeFormatter.ISO_ZONED_DATE_TIME))

  implicit val instantDecoder: Decoder[Instant] =
    Decoder.decodeString.emapTry(str => scala.util.Try(Instant.parse(str)))

  implicit val instantEncoder: Encoder[Instant] =
    Encoder.encodeString.contramap(_.toString)
}
```

### Rules
- **Always** use semi-auto derivation (`deriveEncoder`/`deriveDecoder`) — never full-auto
- Custom decoders only when field names differ or need transformation
- Enum codecs use `emapTry` with the enum's `fromString` method
- Shared serializers for `ZonedDateTime`, `Instant`, etc. live in `repcheck-shared-models`
- DTOs define their codecs in their companion object
