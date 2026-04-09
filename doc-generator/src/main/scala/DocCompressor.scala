package docgenerator

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.stream.Collectors

import scala.jdk.CollectionConverters._

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all._

import com.anthropic.client.AnthropicClient
import com.anthropic.client.okhttp.AnthropicOkHttpClient
import com.anthropic.models.messages.{MessageCreateParams, Model}
import org.slf4j.LoggerFactory

case class CompressionConfig(
  minCompressionRatio: Double,
  anthropicApiKey: String,
  projectRoot: Path,
  outputDir: Path,
)

case class CompressionResult(
  inputFile: Path,
  outputFile: Path,
  originalWords: Int,
  compressedWords: Int,
  compressionRatio: Double,
)

case class InsufficientCompression(
  file: String,
  ratio: Double,
  threshold: Double,
) extends Exception(
      s"Compression of '$file' only achieved ${(ratio * 100).toInt}% reduction, " +
        s"threshold is ${(threshold * 100).toInt}%"
    )

case class MissingApiKey()
    extends Exception(
      "ANTHROPIC_API_KEY environment variable is not set. " +
        "Agent doc generation requires this key. " +
        "Set it and retry your commit."
    )

object DocCompressor extends IOApp {

  private val logger = LoggerFactory.getLogger(getClass)

  private val compressionPrompt: String =
    """Given this documentation file, produce a compressed version optimized
      |for LLM consumption as coding context. Rules:
      |
      |- Preserve ALL code blocks exactly as-is (do not modify any code)
      |- Preserve ALL table data
      |- Preserve "When To Use" sections but compress to terse bullet points
      |- Compress prose explanations into terse, single-line statements
      |- Collapse multi-line code annotations (// ANNOTATION: ...) into single-line comments
      |- Remove introductory/summary paragraphs (redundant with routing index)
      |- Keep "How to Create" sections but compress prose within them
      |- Remove excessive blank lines
      |- Output must be self-contained — an agent reading only this file
      |  must understand the pattern without needing the original
      |
      |Return ONLY the compressed markdown. No preamble, no explanation.""".stripMargin

  private val sourceDirs: List[String] = List(
    "docs/architecture",
    "docs/architecture/acceptance-criteria",
    "docs/templates/annotated",
    "docs/templates/skeletons",
    "docs/templates",
  )

  /** Files with fewer words than this are copied as-is (no LLM compression). */
  private val minWordsForCompression: Int = 100

  private val compressibleExtensions: Set[String] = Set(".md", ".scala", ".txt", ".yml", ".yaml")

  private def discoverSourceFiles(projectRoot: Path): IO[List[String]] =
    IO.blocking {
      sourceDirs.flatMap { dir =>
        val dirPath = projectRoot.resolve(dir)
        if (Files.isDirectory(dirPath)) {
          val stream = Files.walk(dirPath)
          try
            stream
              .collect(Collectors.toList[Path])
              .asScala
              .toList
              .filter(p => Files.isRegularFile(p))
              .filter { p =>
                val name = p.getFileName.toString
                compressibleExtensions.exists(ext => name.endsWith(ext))
              }
              .map(p => projectRoot.relativize(p).toString.replace("\\", "/"))
              .sorted
          finally
            stream.close()
        } else {
          List.empty[String]
        }
      }.distinct
    }

  override def run(args: List[String]): IO[ExitCode] =
    for {
      config      <- loadConfig(args)
      sourceFiles <- discoverSourceFiles(config.projectRoot)
      _           <- IO(logger.info(s"Discovered ${sourceFiles.length} files to compress"))
      client      <- createClient(config.anthropicApiKey)
      results     <- sourceFiles.traverse(file => compressFile(client, config, file))
      _           <- generateIndex(config, results)
      _           <- logSummary(results)
      _           <- validateCompression(config, results)
    } yield ExitCode.Success

  private def loadConfig(args: List[String]): IO[CompressionConfig] = {
    val apiKey = sys.env.get("ANTHROPIC_API_KEY")
    apiKey match {
      case None => IO.raiseError(MissingApiKey())
      case Some(key) =>
        val threshold = args.headOption
          .flatMap(s => scala.util.Try(s.toDouble).toOption)
          .getOrElse(0.20)
        val projectRoot =
          Paths.get(sys.env.getOrElse("PROJECT_ROOT", ".")).toAbsolutePath
        val outputDir = projectRoot.resolve(".claude/agent-docs")
        IO.pure(CompressionConfig(threshold, key, projectRoot, outputDir))
    }
  }

  private def createClient(apiKey: String): IO[AnthropicClient] =
    IO.blocking {
      AnthropicOkHttpClient.builder().apiKey(apiKey).build()
    }

  private def compressFile(
    client: AnthropicClient,
    config: CompressionConfig,
    relativePath: String,
  ): IO[CompressionResult] = {
    val inputPath = config.projectRoot.resolve(relativePath)
    val outputRelative = relativePath
      .replaceFirst("docs/", "")
      .replaceAll("\\.md$", ".compressed.md")
      .replaceAll("\\.scala$", ".compressed.scala")
      .replaceAll("\\.txt$", ".compressed.txt")
      .replaceAll("\\.yml$", ".compressed.yml")
      .replaceAll("\\.yaml$", ".compressed.yaml")
    val outputPath = config.outputDir.resolve(outputRelative)

    for {
      content <- IO.blocking(Files.readString(inputPath))
      originalWords = content.split("\\s+").length
      result <-
        if (originalWords < minWordsForCompression) {
          // Too small to compress — copy as-is with generated header
          IO.blocking {
            Files.createDirectories(outputPath.getParent)
            Files.writeString(
              outputPath,
              s"<!-- GENERATED FILE — DO NOT EDIT. Source: $relativePath -->\n\n$content",
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
            )
          } *> IO(
            logger.info(
              s"  → $outputRelative ($originalWords words, too small to compress — copied as-is)"
            )
          ).as(
            CompressionResult(inputPath, outputPath, originalWords, originalWords, 0.0)
          )
        } else {
          for {
            _          <- IO(logger.info(s"Compressing $relativePath ($originalWords words)..."))
            compressed <- callClaude(client, content)
            compressedWords = compressed.split("\\s+").length
            ratio           = 1.0 - (compressedWords.toDouble / originalWords.toDouble)
            _ <- IO.blocking {
              Files.createDirectories(outputPath.getParent)
              Files.writeString(
                outputPath,
                s"<!-- GENERATED FILE — DO NOT EDIT. Source: $relativePath -->\n\n$compressed",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
              )
            }
            _ <- IO(
              logger.info(
                s"  → $outputRelative ($compressedWords words, ${(ratio * 100).toInt}% reduction)"
              )
            )
          } yield CompressionResult(inputPath, outputPath, originalWords, compressedWords, ratio)
        }
    } yield result
  }

  private def callClaude(
    client: AnthropicClient,
    content: String,
  ): IO[String] =
    IO.blocking {
      val params = MessageCreateParams
        .builder()
        .model(Model.CLAUDE_HAIKU_4_5_20251001)
        .maxTokens(8192)
        .addUserMessage(s"$compressionPrompt\n\n---\n\n$content")
        .build()

      val response = client.messages().create(params)
      response
        .content()
        .asScala
        .collect { case block if block.isText => block.asText().text() }
        .mkString("\n")
    }

  private def generateIndex(
    config: CompressionConfig,
    results: List[CompressionResult],
  ): IO[Unit] = {
    val indexContent = new StringBuilder
    indexContent.append(
      "<!-- GENERATED FILE — DO NOT EDIT. Source: CLAUDE.md -->\n\n"
    )
    indexContent.append("# Agent Docs Index\n\n")
    indexContent.append(
      "Compressed documentation for LLM consumption. See `CLAUDE.md` for routing table.\n\n"
    )
    indexContent.append("| File | Original Words | Compressed Words | Reduction |\n")
    indexContent.append("|------|---------------|-----------------|----------|\n")
    results.foreach { r =>
      val relOutput =
        config.outputDir.relativize(r.outputFile).toString.replace("\\", "/")
      indexContent.append(
        s"| `$relOutput` | ${r.originalWords} | ${r.compressedWords} | ${(r.compressionRatio * 100).toInt}% |\n"
      )
    }
    val totalOriginal   = results.map(_.originalWords).sum
    val totalCompressed = results.map(_.compressedWords).sum
    val totalRatio =
      1.0 - (totalCompressed.toDouble / totalOriginal.toDouble)
    indexContent.append(
      s"| **Total** | **$totalOriginal** | **$totalCompressed** | **${(totalRatio * 100).toInt}%** |\n"
    )

    IO.blocking {
      Files.createDirectories(config.outputDir)
      val _ = Files.writeString(
        config.outputDir.resolve("INDEX.md"),
        indexContent.toString,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
      )
    }
  }

  private def logSummary(results: List[CompressionResult]): IO[Unit] = {
    val totalOriginal   = results.map(_.originalWords).sum
    val totalCompressed = results.map(_.compressedWords).sum
    val totalRatio =
      1.0 - (totalCompressed.toDouble / totalOriginal.toDouble)
    IO(
      logger.info(
        s"Compression complete: $totalOriginal → $totalCompressed words (${(totalRatio * 100).toInt}% reduction)"
      )
    )
  }

  private def validateCompression(
    config: CompressionConfig,
    results: List[CompressionResult],
  ): IO[Unit] = {
    // Only count files that were actually compressed (not copied as-is)
    val compressed      = results.filter(_.originalWords >= minWordsForCompression)
    val totalOriginal   = compressed.map(_.originalWords).sum
    val totalCompressed = compressed.map(_.compressedWords).sum
    val averageRatio =
      if (totalOriginal > 0) { 1.0 - (totalCompressed.toDouble / totalOriginal.toDouble) }
      else { 1.0 }
    val copied = results.length - compressed.length
    if (averageRatio >= config.minCompressionRatio) {
      IO(
        logger.info(
          s"Average compression ${(averageRatio * 100).toInt}% meets ${(config.minCompressionRatio * 100).toInt}% threshold" +
            s" (${compressed.length} compressed, $copied copied as-is)"
        )
      )
    } else {
      IO.raiseError(
        InsufficientCompression(
          s"average across ${compressed.length} compressed files",
          averageRatio,
          config.minCompressionRatio,
        )
      )
    }
  }

}
