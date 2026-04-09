> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 12. LLM Client Abstraction

**Pattern**: Vendor-neutral prompt model assembled by prompt engines. Pluggable adapters convert to vendor-specific SDKs. Supports fan-out to multiple LLM providers.

### Vendor-Neutral Types (in `repcheck-llm-client/models/`)

```scala
enum Role {
  case System, User, Assistant
}

case class LlmMessage(role: Role, content: String)

case class LlmParameters(
  temperature: Double = 0.3,
  maxTokens: Int = 4096
)

// Built by prompt engines, consumed by adapters
case class LlmRequest(
  systemPrompt: String,
  messages: List[LlmMessage],
  parameters: LlmParameters
)

case class TokenUsage(
  inputTokens: Int,
  outputTokens: Int
)

// Returned by adapters, includes provider identification
case class LlmResponse(
  provider: String,
  model: String,
  content: String,
  tokensUsed: TokenUsage
)
```

### Adapter Trait

```scala
trait LlmAdapter[F[_]] {
  def provider: String
  def submit(request: LlmRequest): F[LlmResponse]
}
```

### Dispatcher (fan-out to multiple providers)

```scala
trait LlmDispatcher[F[_]] {
  def submit(request: LlmRequest): F[List[LlmResponse]]
}

class ConfigurableLlmDispatcher[F[_]: Async](
  adapters: List[LlmAdapter[F]]
) extends LlmDispatcher[F] {
  override def submit(request: LlmRequest): F[List[LlmResponse]] =
    adapters.parTraverse(_.submit(request))
}
```

### Claude Adapter (in `repcheck-llm-client/adapters/claude/`)

```scala
import com.anthropic.client.AnthropicClient
import com.anthropic.models.*
import cats.effect.Sync

class ClaudeAdapter[F[_]: Sync](
  apiKey: String,
  model: String = "claude-sonnet-4-20250514"
) extends LlmAdapter[F] {

  override val provider: String = "claude"

  private val client: F[AnthropicClient] = Sync[F].delay {
    AnthropicClient.builder()
      .apiKey(apiKey)
      .build()
  }

  override def submit(request: LlmRequest): F[LlmResponse] =
    client.flatMap { c =>
      Sync[F].blocking {
        val params = MessageCreateParams.builder()
          .model(model)
          .system(request.systemPrompt)
          .messages(request.messages.map(toAnthropicMessage).asJava)
          .maxTokens(request.parameters.maxTokens)
          .temperature(request.parameters.temperature)
          .build()

        val response = c.messages().create(params)

        LlmResponse(
          provider = "claude",
          model = model,
          content = response.content().get(0).text(),
          tokensUsed = TokenUsage(
            inputTokens = response.usage().inputTokens().toInt,
            outputTokens = response.usage().outputTokens().toInt
          )
        )
      }
    }
        .adaptError { case e => LlmCallFailed("claude", model, e) }

  private def toAnthropicMessage(msg: LlmMessage): MessageParam =
    MessageParam.builder()
      .role(msg.role match {
        case Role.User      => MessageParam.Role.USER
        case Role.Assistant => MessageParam.Role.ASSISTANT
        case Role.System    => MessageParam.Role.USER // system handled separately
      })
      .content(msg.content)
      .build()
}
```

### Multi-Provider Storage

When multiple providers analyze the same bill, results are stored per-provider:

```
AlloyDB analyses table — one row per (bill_id, provider):
  (bill_id="hr-1-118", provider="claude", model="claude-sonnet-4-6", result_json=...)
  (bill_id="hr-1-118", provider="gpt", ...)  ← future
```

### Rules
- `LlmRequest`, `LlmResponse`, `LlmAdapter` trait, `LlmDispatcher` live in `repcheck-llm-client/models/` (published)
- Vendor-specific adapters live in `repcheck-llm-client/adapters/{vendor}/`
- Prompt engines build `LlmRequest` — they never touch vendor SDKs
- Adapters convert `LlmRequest` → vendor DTO → call SDK → convert response → `LlmResponse`
- `LlmResponse` always includes `provider` and `model` fields
- `ConfigurableLlmDispatcher` uses `parTraverse` for concurrent fan-out
- Which adapters are active is controlled via config — start with one, add more without code changes
