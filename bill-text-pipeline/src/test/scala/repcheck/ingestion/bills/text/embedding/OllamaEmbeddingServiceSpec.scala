package repcheck.ingestion.bills.text.embedding

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import org.http4s.ember.client.EmberClientBuilder

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

class OllamaEmbeddingServiceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val wireMock = new WireMockServer(
    WireMockConfiguration
      .options()
      .bindAddress("127.0.0.1")
      .dynamicPort()
  )

  private val noopLogger: PipelineLogger[IO] = new PipelineLogger[IO] {
    def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  private lazy val httpClient = EmberClientBuilder
    .default[IO]
    .withTimeout(5.seconds)
    .build

  private def config: EmbeddingConfig =
    EmbeddingConfig(
      baseUrl = s"http://127.0.0.1:${wireMock.port().toString}",
      modelName = "qwen3-embedding",
      dimensions = 4,
      timeoutSeconds = 5,
    )

  private def service: OllamaEmbeddingService[IO] =
    new OllamaEmbeddingService[IO](
      client = httpClient.allocated.unsafeRunSync()._1,
      config = config,
      logger = noopLogger,
    )

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    wireMock.resetAll()
  }

  private def stubEmbedding(embedding: List[Float]): Unit = {
    val embeddingJson = embedding.map(_.toString).mkString("[", ",", "]")
    val _ = wireMock.stubFor(
      post(urlEqualTo("/api/embed"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(s"""{"embeddings":[$embeddingJson]}""")
        )
    )
  }

  "generateEmbedding" should "return embedding from Ollama response" in {
    val expected = List(0.1f, 0.2f, 0.3f, 0.4f)
    stubEmbedding(expected)

    val result = service.generateEmbedding("Some bill text").unsafeRunSync()

    val _ = result.isDefined shouldBe true
    result.map(_.toList) shouldBe Some(expected)
  }

  it should "return None for empty text" in {
    val result = service.generateEmbedding("").unsafeRunSync()
    result shouldBe None
  }

  it should "return None for whitespace-only text" in {
    val result = service.generateEmbedding("   ").unsafeRunSync()
    result shouldBe None
  }

  it should "send correct model name and dimensions in request" in {
    stubEmbedding(List(0.1f, 0.2f, 0.3f, 0.4f))

    val _ = service.generateEmbedding("test text").unsafeRunSync()

    wireMock.verify(
      postRequestedFor(urlEqualTo("/api/embed"))
        .withRequestBody(matchingJsonPath("$.model", equalTo("qwen3-embedding")))
        .withRequestBody(matchingJsonPath("$.dimensions", equalTo("4")))
        .withRequestBody(matchingJsonPath("$.input", equalTo("test text")))
    )
  }

  it should "return None when Ollama returns HTTP error (graceful degradation)" in {
    wireMock.stubFor(
      post(urlEqualTo("/api/embed"))
        .willReturn(aResponse().withStatus(500).withBody("Internal Server Error"))
    )

    val result = service.generateEmbedding("some text").unsafeRunSync()
    result shouldBe None
  }

  it should "return None when Ollama is unreachable (graceful degradation)" in {
    val unreachableConfig = config.copy(baseUrl = "http://127.0.0.1:1")
    val unreachableService = new OllamaEmbeddingService[IO](
      client = httpClient.allocated.unsafeRunSync()._1,
      config = unreachableConfig,
      logger = noopLogger,
    )

    val result = unreachableService.generateEmbedding("some text").unsafeRunSync()
    result shouldBe None
  }

  it should "raise EmbeddingGenerationFailed on dimension mismatch via callOllama" in {
    // Ollama returns 3-dim vector but config expects 4
    val wrongDimJson = """{"embeddings":[[0.1,0.2,0.3]]}"""
    wireMock.stubFor(
      post(urlEqualTo("/api/embed"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(wrongDimJson)
        )
    )

    // callOllama should raise, but generateEmbedding swallows it → None
    val result = service.generateEmbedding("some text").unsafeRunSync()
    result shouldBe None
  }

  it should "raise EmbeddingGenerationFailed when callOllama gets dimension mismatch" in {
    val wrongDimJson = """{"embeddings":[[0.1,0.2,0.3]]}"""
    wireMock.stubFor(
      post(urlEqualTo("/api/embed"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(wrongDimJson)
        )
    )

    val error = service.callOllama("some text").attempt.unsafeRunSync()
    val _     = error.isLeft shouldBe true
    error.swap.getOrElse(fail("Expected error")) shouldBe a[EmbeddingGenerationFailed]
  }

  it should "raise EmbeddingGenerationFailed when Ollama returns empty embeddings array" in {
    wireMock.stubFor(
      post(urlEqualTo("/api/embed"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"embeddings":[]}""")
        )
    )

    val error = service.callOllama("some text").attempt.unsafeRunSync()
    val _     = error.isLeft shouldBe true
    error.swap.getOrElse(fail("Expected error")) shouldBe a[EmbeddingGenerationFailed]
  }

  it should "produce deterministic results for same input" in {
    val embedding = List(0.1f, 0.2f, 0.3f, 0.4f)
    stubEmbedding(embedding)

    val result1 = service.generateEmbedding("same text").unsafeRunSync()
    val result2 = service.generateEmbedding("same text").unsafeRunSync()

    val _ = result1.isDefined shouldBe true
    result1.map(_.toList) shouldBe result2.map(_.toList)
  }

}
