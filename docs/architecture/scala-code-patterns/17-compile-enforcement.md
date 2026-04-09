> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 18. Compile-Time Enforcement

These tools enforce the patterns in this document at compile time. Violations fail the build.

### 19a. WartRemover

Catches common Scala anti-patterns.

```scala
// project/plugins.sbt
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.1.6")

// build.sbt (in commonSettings)
wartremoverErrors ++= Warts.allBut(
  Wart.Any,           // too strict for Java interop
  Wart.Nothing,       // too strict for sealed traits
  Wart.Serializable   // not relevant
)
```

**What it catches**:
| Wart | Prevents |
|---|---|
| `Wart.Null` | Using `null` — use `Option` instead |
| `Wart.Var` | Mutable `var` declarations |
| `Wart.Return` | `return` statements |
| `Wart.AsInstanceOf` | Unsafe casts |
| `Wart.IsInstanceOf` | Runtime type checks — use pattern matching |
| `Wart.StringPlusAny` | String concatenation with non-strings — use string interpolation |
| `Wart.Throw` | Bare `throw` — use `F.raiseError` instead |
| `Wart.MutableDataStructures` | Mutable collections |

### 19b. Scalafix

Custom lint rules for project-specific patterns.

```scala
// project/plugins.sbt
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")

// .scalafix.conf
rules = [
  OrganizeImports,
  NoAutoTupling,
  RemoveUnused,
  LeakingImplicitClassVal
]

OrganizeImports {
  groupedImports = Merge
  groups = [
    "re:javax?\\.",
    "scala.",
    "cats.",
    "io.circe.",
    "org.http4s.",
    "fs2.",
    "com.google.",
    "*",
    "com.repcheck."
  ]
}
```

**Custom Scalafix rules** (create in `project/scalafix/`):

| Rule | Enforces |
|---|---|
| `NoHardcodedTableNames` | All AlloyDB table refs must use `Tables.*` constants |
| `NoDtoInPipelineCode` | DTO types must not appear in pipeline business logic — only DOs |
| `NoDirectIoInLibrary` | Library projects must not reference `cats.effect.IO` directly — only `F[_]` |
| `NoUnsafeRunSync` | `unsafeRunSync` only allowed in `src/test/` |

### 19c. ArchUnit (Architecture Tests)

Enforces dependency direction via tests.

```scala
// In each repo's test suite
class ArchitectureSpec extends AnyFlatSpec with Matchers {

  "models package" should "not depend on pipeline code" in {
    // models/ project should never import from pipelines/
    val modelsClasses = classesIn("com.repcheck.dataingestion.models")
    modelsClasses.foreach { cls =>
      cls.imports should not contain regex("com\\.repcheck\\.dataingestion\\.pipelines\\..*")
    }
  }

  "pipeline code" should "not use DTOs directly" in {
    // Pipeline business logic should only reference DOs
    val pipelineClasses = classesIn("com.repcheck.dataingestion.pipelines")
    pipelineClasses.foreach { cls =>
      cls.imports should not contain regex(".*ApiDTO$")
      cls.imports should not contain regex(".*DbDTO$")
    }
  }
}
```

### 19d. tpolecat (Already in Use)

Strict compiler flags — already configured in the project.

```scala
// project/plugins.sbt
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.4.4")

// Key flags it enables:
// -Wunused:imports, -Wunused:params, -Wunused:privates
// -Xfatal-warnings (warnings become errors)
// -Wvalue-discard (unused values flagged)
```

### 19e. SBT Build Guard

Prevent accidental cross-dependency violations in multi-project builds:

```scala
// In build.sbt — models project must NOT depend on pipeline libraries
lazy val models = (project in file("models"))
  .settings(
    commonSettings,
    name := "repcheck-data-ingestion-models",
    // Explicitly: NO http4s, NO fs2, NO firebase, NO pureconfig
    // Only circe + shared-models
    libraryDependencies ++= circe ++ Seq(
      "com.repcheck" %% "repcheck-shared-models" % Versions.sharedModels
    )
  )
```

### Enforcement Summary

| Tool | What It Enforces | Failure Mode |
|---|---|---|
| **WartRemover** | No nulls, vars, throws, mutable state | Compile error |
| **Scalafix** | Import organization, no hardcoded strings, DTO/DO separation | Compile error or CI lint step |
| **ArchUnit** | Package dependency direction, layer isolation | Test failure |
| **tpolecat** | Unused imports/params, fatal warnings | Compile error |
| **SBT structure** | Models project can't depend on runtime libraries | Build won't resolve if violated |

### CI Integration

```yaml
# .github/workflows/ci.yml (per repo)
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: '21'
          distribution: 'temurin'
      - name: Compile with WartRemover
        run: sbt compile
      - name: Run Scalafix checks
        run: sbt "scalafixAll --check"
      - name: Run tests (includes ArchUnit)
        run: sbt test
```
