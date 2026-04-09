<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/llm-client-adapter.scala -->

```markdown
# RepCheck LLM Client Adapter

Vendor-neutral LLM abstraction. Build prompts in neutral format, pluggable adapters convert to vendor-specific DTOs.

## Vendor-Neutral Models

```scala
enum Role {
  case System, User, Assistant
}

object Role {
  given Encoder[Role] = Encoder.encodeString.contramap(_.toString.toLowerCase)
  given Decoder[Role] = Decoder.decodeString.emap {
    case "system"    => Right(Role.System)
    case "user"      => Right(Role.User)
    case "assistant" => Right(Role.Assistant)
    case other       => Left(s"Unknown role: $other")
  }
}

final case class LlmMessage(role: Role, content: String)
object LlmMessage {
  given Encoder[LlmMessage] = deriveEncoder
  given Decoder[LlmMessage] = deriveDecoder
}

final case class LlmParameters(
    temperature: Double = 0.7,
    maxTokens: Int = 4096
)
object LlmParameters {
  given Encoder[LlmParameters] = deriveEncoder
  given Decoder[LlmParameters] = deriveDecoder
}

final case class LlmRequest(
    systemPrompt: String,
    messages: List[LlmMessage],
    parameters: LlmParameters = LlmParameters()
)
object LlmRequest {
  given Encoder[LlmRequest] = deriveEncoder
  given Decoder[LlmRequest] = deriveDecoder
}

final case class TokenUsage(
    inputTokens: Int,
    outputTokens: Int
)
object TokenUsage {
  given Encoder[TokenUsage] = deriveEncoder
  given Decoder[TokenUsage] = deriveDecoder
}

final case class LlmResponse(
    provider: String,
    content: String,
    model: String,
    tokensUsed: TokenUsage
)
object LlmResponse {
  given Encoder[LlmResponse] = deriveEncoder
  given Decoder[LlmResponse] = deriveDecoder
}
```

## Adapter Trait

```scala
trait LlmAdapter[F[_]] {
  def provider: String
  def submit(request: LlmRequest): F[LlmResponse]
}
```

Each provider implements `LlmAdapter`: converts `LlmRequest` → vendor DTO → SDK call → `LlmResponse`.

## Claude Adapter

```scala
object ClaudeAdapter {

  final case class ClaudeConfig(
      apiKey: String,
      model: String = "claude-sonnet-4-20250514",
      retry: RetryConfig = RetryConfig(
        maxRetries = 2,
        initialBackoff = 1.second,
        maxBackoff = 60.seconds,
        timeout = 120.seconds
      )
  )

  def make[F[_]: Async](
      config: ClaudeConfig,
      classifier: ErrorClassifier
  ): LlmAdapter[F] =
    new LlmAdapter[F] {
      val provider: String = "claude"

      def submit(request: LlmRequest): F[LlmResponse] = {
        val operation: F[LlmResponse] = Async[F].blocking {
          // TODO: Convert LlmRequest → Anthropic SDK MessageCreateParams
          //
          // val client = AnthropicClient.builder()
          //   .apiKey(config.apiKey)
          //   .build()
          //
          // val message = MessageCreateParams.builder()
          //   .model(config.model)
          //   .system(request.systemPrompt)
          //   .messages(request.messages.map { msg =>
          //     MessageParam.builder()
          //       .role(msg.role match { case Role.User => "user"; case _ => "assistant" })
          //       .content(msg.content)
          //       .build()
          //   }.asJava)
          //   .maxTokens(request.parameters.maxTokens)
          //   .temperature(request.parameters.temperature)
          //   .build()
          //
          // val response = client.messages().create(message)
          //
          // LlmResponse(
          //   provider = "claude",
          //   content = response.content().get(0).text(),
          //   model = config.model,
          //   tokensUsed = TokenUsage(
          //     inputTokens = response.usage().inputTokens(),
          //     outputTokens = response.usage().outputTokens()
          //   )
          // )
          ???
        }

        RetryWrapper.withRetry[F, LlmResponse](
          config.retry,
          classifier,
          "llm-claude"
        )(operation)
      }
    }

  import scala.concurrent.duration.*
}
```

## GPT Adapter

```scala
object GptAdapter {

  final case class GptConfig(
      apiKey: String,
      model: String = "gpt-4o",
      retry: RetryConfig = RetryConfig(
        maxRetries = 2,
        initialBackoff = 1.second,
        maxBackoff = 60.seconds,
        timeout = 120.seconds
      )
  )

  def make[F[_]: Async](
      config: GptConfig,
      classifier: ErrorClassifier
  ): LlmAdapter[F] =
    new LlmAdapter[F] {
      val provider: String = "gpt"

      def submit(request: LlmRequest): F[LlmResponse] = {
        val operation: F[LlmResponse] = Async[F].blocking {
          // TODO: Convert LlmRequest → OpenAI ChatCompletionRequest
          //
          // val client = OpenAI.builder().apiKey(config.apiKey).build()
          //
          // val messages = (
          //   ChatMessage.system(request.systemPrompt) ::
          //   request.messages.map { msg =>
          //     msg.role match {
          //       case Role.User      => ChatMessage.user(msg.content)
          //       case Role.Assistant => ChatMessage.assistant(msg.content)
          //       case Role.System    => ChatMessage.system(msg.content)
          //     }
          //   }
          // ).asJava
          //
          // val params = ChatCompletionCreateParams.builder()
          //   .model(config.model)
          //   .messages(messages)
          //   .maxTokens(request.parameters.maxTokens)
          //   .temperature(request.parameters.temperature)
          //   .build()
          //
          // val response = client.chat().completions().create(params)
          ???
        }

        RetryWrapper.withRetry[F, LlmResponse](
          config.retry,
          classifier,
          "llm-gpt"
        )(operation)
      }
    }

  import scala.concurrent.duration.*
}
```

## LLM Dispatcher

```scala
trait LlmDispatcher[F[_]] {
  def submit(request: LlmRequest): F[List[LlmResponse]]
}

object LlmDispatcher {

  def make[F[_]: Async](adapters: List[LlmAdapter[F]]): LlmDispatcher[F] =
    new LlmDispatcher[F] {
      def submit(request: LlmRequest): F[List[LlmResponse]] =
        adapters.parTraverse(_.submit(request))
    }
}
```

Submits same `LlmRequest` to all configured adapters concurrently. Returns list of `LlmResponse` (one per provider) for multi-provider comparison.

## Architecture

- **LlmRequest/LlmResponse**: vendor-neutral, live in `llm-client/models/`
- **LlmAdapter[F]**: trait per provider with `submit` method
- **Per-provider retry**: wrapped via `RetryWrapper` with configurable backoff & timeout
- **Concurrent dispatch**: `LlmDispatcher` fans out via `parTraverse`
- **Direct SDK integration**: Anthropic Java SDK, OpenAI SDK behind adapters
- **Provider tracking**: response includes `provider` + `model` fields for attribution
```