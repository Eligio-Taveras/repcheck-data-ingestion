// =============================================================================
// RepCheck Skeleton: Vendor-Neutral LLM Client Adapter
// Repo: repcheck-llm-client
// =============================================================================
//
// PURPOSE: Vendor-neutral LLM abstraction. Build prompts in a neutral format,
// then pluggable adapters convert to vendor-specific DTOs (Claude, GPT, etc.).
//
// KEY DECISIONS (from Q&A):
// - LlmRequest/LlmResponse are vendor-neutral (live in llm-client/models/)
// - LlmAdapter[F] trait per provider with submit method
// - LlmDispatcher fans out concurrently to all configured adapters
// - LlmResponse includes provider + model fields for multi-provider comparison
// - Direct SDK integration (Anthropic Java SDK, OpenAI SDK) behind adapters
// - Uses retry wrapper per provider
// =============================================================================

package repcheck.llm.client

import cats.effect.Async
import cats.syntax.all.*

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*

import repcheck.pipeline.models.retry.{ErrorClassifier, RetryConfig, RetryWrapper}

// ---------------------------------------------------------------------------
// Vendor-Neutral Models (in repcheck-llm-client/models/)
// ---------------------------------------------------------------------------

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

/** Vendor-neutral LLM request.
  * Built by the prompt engine, consumed by adapters.
  */
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

/** Vendor-neutral LLM response.
  * Includes provider and model so downstream can distinguish which LLM produced it.
  */
final case class LlmResponse(
    provider: String,    // "claude", "gpt", "gemini"
    content: String,
    model: String,       // "claude-sonnet-4-20250514", "gpt-4o"
    tokensUsed: TokenUsage
)
object LlmResponse {
  given Encoder[LlmResponse] = deriveEncoder
  given Decoder[LlmResponse] = deriveDecoder
}

// ---------------------------------------------------------------------------
// Adapter Trait (in repcheck-llm-client/models/)
// ---------------------------------------------------------------------------

/** Each LLM provider implements this trait.
  * The adapter converts LlmRequest → vendor DTO → calls SDK → converts response → LlmResponse
  */
trait LlmAdapter[F[_]] {
  def provider: String
  def submit(request: LlmRequest): F[LlmResponse]
}

// ---------------------------------------------------------------------------
// Claude Adapter (in repcheck-llm-client/adapters/claude/)
// ---------------------------------------------------------------------------

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
          // Convert Anthropic response → LlmResponse
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

// ---------------------------------------------------------------------------
// GPT Adapter (in repcheck-llm-client/adapters/gpt/)
// ---------------------------------------------------------------------------

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
          //
          // Convert OpenAI response → LlmResponse
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

// ---------------------------------------------------------------------------
// LLM Dispatcher — concurrent fan-out to all configured providers
// ---------------------------------------------------------------------------

/** Submits the same LlmRequest to all configured adapters concurrently.
  * Returns a list of LlmResponse, one per provider.
  *
  * This enables multi-provider analysis: a bill can have both a Claude
  * analysis and a GPT analysis stored side by side.
  */
trait LlmDispatcher[F[_]] {
  def submit(request: LlmRequest): F[List[LlmResponse]]
}

object LlmDispatcher {

  def make[F[_]: Async](adapters: List[LlmAdapter[F]]): LlmDispatcher[F] =
    new LlmDispatcher[F] {
      def submit(request: LlmRequest): F[List[LlmResponse]] =
        // Fan out concurrently to all adapters
        adapters.parTraverse(_.submit(request))
    }
}
