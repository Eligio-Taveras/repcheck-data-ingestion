> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 10. Pub/Sub Messaging

**Pattern**: Case classes in `repcheck-pipeline-models` with Circe codecs. Generic publisher/subscriber helpers.

### Event Definitions (in `repcheck-pipeline-models`)

```scala
import java.time.Instant
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class BillTextAvailableEvent(
  billId: String,
  textUrl: String,
  format: String,
  timestamp: Instant
)
object BillTextAvailableEvent {
  implicit val decoder: Decoder[BillTextAvailableEvent] = deriveDecoder
  implicit val encoder: Encoder[BillTextAvailableEvent] = deriveEncoder
}

case class VoteRecordedEvent(
  voteId: String,
  billId: String,
  chamber: String,
  date: Instant,
  timestamp: Instant
)
object VoteRecordedEvent {
  implicit val decoder: Decoder[VoteRecordedEvent] = deriveDecoder
  implicit val encoder: Encoder[VoteRecordedEvent] = deriveEncoder
}

case class AnalysisCompletedEvent(
  billId: String,
  analysisId: String,
  topics: List[String],
  modelUsed: String,
  provider: String,
  timestamp: Instant
)
object AnalysisCompletedEvent {
  implicit val decoder: Decoder[AnalysisCompletedEvent] = deriveDecoder
  implicit val encoder: Encoder[AnalysisCompletedEvent] = deriveEncoder
}

case class UserProfileUpdatedEvent(
  userId: String,
  topicsChanged: List[String],
  timestamp: Instant
)
object UserProfileUpdatedEvent {
  implicit val decoder: Decoder[UserProfileUpdatedEvent] = deriveDecoder
  implicit val encoder: Encoder[UserProfileUpdatedEvent] = deriveEncoder
}
```

### Topic Constants (in `repcheck-pipeline-models`)

```scala
object Topics {
  val BillEvents     = "bill-events"
  val VoteEvents     = "vote-events"
  val AnalysisEvents = "analysis-events"
  val UserEvents     = "user-events"
}
```

### Generic Publisher

```scala
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import com.google.protobuf.ByteString
import io.circe.Encoder
import io.circe.syntax.*
import cats.effect.Sync

trait EventPublisher[F[_]] {
  def publish[E: Encoder](topic: String, event: E): F[String]
}

class PubSubEventPublisher[F[_]: Sync](projectId: String) extends EventPublisher[F] {
  override def publish[E: Encoder](topic: String, event: E): F[String] =
    Sync[F].blocking {
      val topicName = TopicName.of(projectId, topic)
      val publisher = Publisher.newBuilder(topicName).build()
      try {
        val json = event.asJson.noSpaces
        val message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(json))
          .build()
        publisher.publish(message).get()
      } finally {
        publisher.shutdown()
      }
    }
}
```

### Generic Subscriber

```scala
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import io.circe.Decoder
import io.circe.parser.decode
import cats.effect.Async

trait EventSubscriber[F[_]] {
  def subscribe[E: Decoder](
    subscription: String,
    handler: E => F[Unit]
  ): F[Unit]
}
```

### Rules
- All events have a `timestamp: Instant` field
- Events are serialized to JSON via Circe, sent as Pub/Sub message data
- Topic names referenced via `Topics` constants
- Only emit events that have downstream consumers (see SYSTEM_DESIGN.md event catalog)
- Publisher/subscriber helpers live in `repcheck-pipeline-models`
