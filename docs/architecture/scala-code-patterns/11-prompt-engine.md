> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 11. Prompt Engine

**Pattern**: All prompt fragments live in GCS as serializable instruction blocks. No hardcoded prompts in code. The code is purely a loader + assembler.

### Core Types (base traits in `repcheck-shared-models`)

```scala
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

// Atomic unit of prompt composition — stored as JSON/YAML in GCS
case class InstructionBlock(
  name: String,
  stage: String,
  weight: Double,
  version: String,
  content: String
)
object InstructionBlock {
  implicit val decoder: Decoder[InstructionBlock] = deriveDecoder
  implicit val encoder: Encoder[InstructionBlock] = deriveEncoder
}

// Defines which blocks compose a specific analysis type
case class PromptProfile(
  name: String,
  chain: List[StageConfig]
)
object PromptProfile {
  implicit val decoder: Decoder[PromptProfile] = deriveDecoder
  implicit val encoder: Encoder[PromptProfile] = deriveEncoder
}

case class StageConfig(
  stage: String,
  blocks: List[String],  // block names to include
  weight: Double
)
object StageConfig {
  implicit val decoder: Decoder[StageConfig] = deriveDecoder
  implicit val encoder: Encoder[StageConfig] = deriveEncoder
}
```

### Chain Assembler Trait (in `repcheck-shared-models`)

```scala
trait ChainAssembler[F[_]] {
  def assemble(
    profile: PromptProfile,
    blocks: Map[String, InstructionBlock],
    runtimeContext: Map[String, String]  // e.g., bill text, user prefs injected at runtime
  ): F[String]
}
```

### Weight Translation

```scala
object WeightTranslator {
  def applyWeight(content: String, weight: Double): String = weight match {
    case w if w >= 1.0  => s"CRITICAL INSTRUCTION — You MUST follow this exactly:\n$content"
    case w if w >= 0.7  => content  // standard framing, no modification
    case w if w >= 0.3  => s"When possible, consider the following:\n$content"
    case _              => s"Optional guidance (apply if relevant):\n$content"
  }
}
```

### Block Loader (in prompt-engine repos)

```scala
class GcsBlockLoader[F[_]: Sync](
  gcsClient: GcsClient[F],
  bucket: String,
  basePath: String  // e.g., "bills/blocks" or "users/blocks"
) {
  def loadAllBlocks(): F[Map[String, InstructionBlock]] =
    for {
      paths  <- gcsClient.listObjects(bucket, basePath)
      blocks <- paths.traverse { path =>
        gcsClient.readObject(bucket, path)
          .flatMap(json => Sync[F].fromEither(decode[InstructionBlock](json)))
      }
    } yield blocks.map(b => b.name -> b).toMap

  def loadProfile(profileName: String): F[PromptProfile] =
    gcsClient.readObject(bucket, s"profiles/$profileName.json")
      .flatMap(json => Sync[F].fromEither(decode[PromptProfile](json)))
}
```

### Rules
- **Zero** prompt content in code — all fragments live in GCS
- Prompt engine repos are purely loaders + assemblers
- `InstructionBlock`, `PromptProfile`, `StageConfig`, `ChainAssembler` trait live in `repcheck-shared-models`
- Weight translation converts numeric weights to prompt language patterns
- Runtime context (bill text, user preferences) is injected during assembly, not stored in GCS
- GCS bucket: `repcheck-prompt-configs` with paths `bills/blocks/`, `bills/profiles/`, `users/blocks/`, `users/profiles/`
