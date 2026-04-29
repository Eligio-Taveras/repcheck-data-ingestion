<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/scala-code-patterns/11-prompt-engine.md -->

# 11. Prompt Engine

**Pattern**: All prompt fragments live in GCS as serializable instruction blocks. No hardcoded prompts in code — code is purely loader + assembler.

## Core Types (base traits in `repcheck-shared-models`)

```scala
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

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
  blocks: List[String],
  weight: Double
)
object StageConfig {
  implicit val decoder: Decoder[StageConfig] = deriveDecoder
  implicit val encoder: Encoder[StageConfig] = deriveEncoder
}
```

## Chain Assembler Trait (in `repcheck-shared-models`)

```scala
trait ChainAssembler[F[_]] {
  def assemble(
    profile: PromptProfile,
    blocks: Map[String, InstructionBlock],
    runtimeContext: Map[String, String]
  ): F[String]
}
```

## Weight Translation

```scala
object WeightTranslator {
  def applyWeight(content: String, weight: Double): String = weight match {
    case w if w >= 1.0  => s"CRITICAL INSTRUCTION — You MUST follow this exactly:\n$content"
    case w if w >= 0.7  => content
    case w if w >= 0.3  => s"When possible, consider the following:\n$content"
    case _              => s"Optional guidance (apply if relevant):\n$content"
  }
}
```

## Block Loader (in prompt-engine repos)

```scala
class GcsBlockLoader[F[_]: Sync](
  gcsClient: GcsClient[F],
  bucket: String,
  basePath: String
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

## Rules
- Zero prompt content in code — all fragments live in GCS
- Prompt engine repos are loaders + assemblers only
- `InstructionBlock`, `PromptProfile`, `StageConfig`, `ChainAssembler` in `repcheck-shared-models`
- Weight translation converts numeric weights to prompt language
- Runtime context injected during assembly, not stored in GCS
- GCS bucket: `repcheck-prompt-configs` with paths `bills/blocks/`, `bills/profiles/`, `users/blocks/`, `users/profiles/`