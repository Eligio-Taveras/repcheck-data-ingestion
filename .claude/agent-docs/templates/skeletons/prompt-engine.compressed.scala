<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/prompt-engine.scala -->

```markdown
# RepCheck Skeleton: Prompt Engine — Fragment Composition

**Repo:** repcheck-prompt-engine-bills, repcheck-prompt-engine-users

**Purpose:** Loads prompt fragments from GCS, sorts by priority, composes into final prompt string. ALL fragments live in GCS — none in code.

**Key Decisions:**
- PromptFragment trait with key, priority, content, render()
- Serializable/deserializable via Circe
- All fragments stored in GCS with semver in filenames
- PromptBuilder loads + sorts by priority + renders final prompt
- Version configurable per app, default set by CI on release
- User preferences (from AlloyDB) woven in by scoring prompt engine

---

## Prompt Fragment — the atomic unit of prompt composition

```scala
final case class PromptFragment(
    key: String,
    priority: Int,
    content: String,
    metadata: Option[PromptFragmentMetadata] = None
) {
  /** Render this fragment. Override for fragments needing variable interpolation. */
  def render(): String = content
}

final case class PromptFragmentMetadata(
    description: String,
    author: String,
    version: String // semver, matches the filename
)

object PromptFragment {
  given Encoder[PromptFragment] = deriveEncoder
  given Decoder[PromptFragment] = deriveDecoder
}

object PromptFragmentMetadata {
  given Encoder[PromptFragmentMetadata] = deriveEncoder
  given Decoder[PromptFragmentMetadata] = deriveDecoder
}
```

---

## GCS Prompt Fragment Structure

**Bucket:** `repcheck-prompt-configs/`

```
bills/
  base-system-instruction-v1.0.0.json     (priority: 0)
  bill-structure-analysis-v1.1.0.json     (priority: 10)
  policy-area-classification-v1.0.0.json  (priority: 20)
  impact-assessment-v1.2.0.json           (priority: 30)
  pork-rider-detection-v1.0.0.json        (priority: 40)
  fiscal-estimate-v1.0.0.json             (priority: 50)
users/
  base-system-instruction-v1.0.0.json     (priority: 0)
  preference-extraction-v1.0.0.json       (priority: 10)
  alignment-criteria-v1.1.0.json          (priority: 20)
```

**Fragment JSON Format:**
```json
{
  "key": "base-system-instruction",
  "priority": 0,
  "content": "You are an expert legislative analyst...",
  "metadata": {
    "description": "Base system prompt for bill analysis",
    "author": "eligio",
    "version": "v1.0.0"
  }
}
```

---

## Prompt Builder — loads fragments from GCS and assembles them

```scala
trait PromptBuilder[F[_]] {
  /** Load all fragments for a category and assemble the final prompt.
    *
    * @param category "bills" or "users"
    * @param version  Semver string (e.g., "v1.2.0") — filters fragments by version
    * @return Assembled prompt string, fragments ordered by priority
    */
  def assemble(category: String, version: String): F[String]

  /** Load all fragments for a category (raw, not assembled).
    * Useful when you need to inspect or modify fragments before assembly.
    */
  def loadFragments(category: String, version: String): F[List[PromptFragment]]
}

object PromptBuilder {

  final case class PromptBuilderConfig(
      bucket: String = "repcheck-prompt-configs",
      // Version defaults — set by CI on release, overridable via app config
      defaultBillsVersion: String,
      defaultUsersVersion: String
  )

  /** Create a PromptBuilder that reads from GCS.
    *
    * @param gcsClient GCS reader (from gcs-reader.scala skeleton)
    * @param config    Builder configuration
    */
  def make[F[_]: Sync](
      gcsClient: GcsClient[F],
      config: PromptBuilderConfig
  ): PromptBuilder[F] =
    new PromptBuilder[F] {

      def assemble(category: String, version: String): F[String] =
        loadFragments(category, version).map { fragments =>
          fragments
            .sortBy(_.priority)
            .map(_.render())
            .mkString("\n\n")
        }

      def loadFragments(
          category: String,
          version: String
      ): F[List[PromptFragment]] =
        for {
          // List all fragment files in the category directory matching the version
          paths <- gcsClient.listVersioned(
            config.bucket,
            s"$category/",
            version
          )

          // Load and deserialize each fragment
          fragments <- paths.traverse { path =>
            gcsClient.readJson[PromptFragment](config.bucket, path)
          }
        } yield fragments
    }
}
```

---

## GCS Client trait

```scala
/** Minimal GCS client interface needed by the prompt builder.
  * Full implementation in gcs-reader.scala skeleton.
  */
trait GcsClient[F[_]] {
  def readJson[T: Decoder](bucket: String, path: String): F[T]
  def listVersioned(bucket: String, prefix: String, version: String): F[List[String]]
}
```

---

## User-Aware Prompt Assembly

```scala
/** Extended prompt builder that weaves user preferences into the prompt.
  * Used by repcheck-prompt-engine-users for the scoring pipeline.
  *
  * User preferences come from AlloyDB (via Doobie repository),
  * prompt fragments come from GCS.
  */
trait UserAwarePromptBuilder[F[_]] {
  /** Assemble a scoring prompt for a specific user.
    *
    * @param userId  User whose preferences to load
    * @param version Prompt fragment version
    * @return Assembled prompt with user preferences interpolated
    */
  def assembleForUser(userId: String, version: String): F[String]
}
```

**Implementation TODO:**
1. Load base prompt fragments from GCS via PromptBuilder
2. Load user preferences from AlloyDB via UserRepository
3. Create dynamic PromptFragment with user preferences serialized
4. Insert user fragment at appropriate priority position
5. Render final assembled prompt
```