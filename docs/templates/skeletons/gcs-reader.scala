// =============================================================================
// RepCheck Skeleton: GCS Client
// Repo: repcheck-pipeline-models (shared library)
// =============================================================================
//
// PURPOSE: Wraps Google Cloud Storage Java SDK in Sync[F] for reading
// versioned JSON files (prompt fragments, snapshots, workflow definitions).
//
// KEY DECISIONS (from Q&A):
// - Raw GCS Java SDK wrapped in Sync[F]
// - Semver in filenames (e.g., base-system-instruction-v1.2.0.json)
// - Version filtering by scanning filenames
// - Uses retry wrapper for all GCS operations
// =============================================================================

package repcheck.pipeline.models.gcs

import cats.effect.{Resource, Sync}
import cats.syntax.all.*

import io.circe.{Decoder, parser}

import scala.jdk.CollectionConverters.*

import repcheck.pipeline.models.retry.{ErrorClassifier, RetryConfig, RetryWrapper}

// ---------------------------------------------------------------------------
// GCS Client Trait
// ---------------------------------------------------------------------------

trait GcsClient[F[_]] {

  /** Read a JSON file from GCS and deserialize to T. */
  def readJson[T: Decoder](bucket: String, path: String): F[T]

  /** Read raw bytes from GCS. */
  def readBytes(bucket: String, path: String): F[Array[Byte]]

  /** Write JSON to GCS with a versioned filename. */
  def writeJson[T: io.circe.Encoder](
      bucket: String,
      path: String,
      value: T
  ): F[Unit]

  /** List all objects under a prefix that match a specific semver version.
    *
    * Example: listVersioned("my-bucket", "bills/", "v1.2.0")
    * Returns: List("bills/base-system-instruction-v1.2.0.json",
    *               "bills/policy-area-classification-v1.2.0.json")
    */
  def listVersioned(
      bucket: String,
      prefix: String,
      version: String
  ): F[List[String]]

  /** List all objects under a prefix (no version filter). */
  def listAll(bucket: String, prefix: String): F[List[String]]
}

// ---------------------------------------------------------------------------
// Google Cloud Storage SDK Implementation
// ---------------------------------------------------------------------------

object GcsClient {

  final case class GcsConfig(
      retry: RetryConfig = RetryConfig(
        maxRetries = 3,
        initialBackoff = 100.millis,
        maxBackoff = 15.seconds,
        timeout = 15.seconds
      )
  )

  import scala.concurrent.duration.*

  /** Create a GcsClient Resource managing the underlying Storage instance lifecycle. */
  def make[F[_]: Sync](
      config: GcsConfig,
      classifier: ErrorClassifier
  ): Resource[F, GcsClient[F]] =
    Resource
      .make(
        Sync[F].blocking {
          // TODO: Replace with real initialization
          // com.google.cloud.storage.StorageOptions.getDefaultInstance.getService
          ???: com.google.cloud.storage.Storage
        }
      )(storage =>
        Sync[F].blocking {
          // Storage client doesn't need explicit close, but we keep the
          // Resource pattern for consistency
        }
      )
      .map { storage =>
        new GcsClient[F] {

          def readJson[T: Decoder](bucket: String, path: String): F[T] = {
            val operation: F[T] = Sync[F]
              .blocking {
                // TODO: val blob = storage.get(bucket, path)
                // TODO: new String(blob.getContent(), java.nio.charset.StandardCharsets.UTF_8)
                ???: String
              }
              .flatMap { jsonStr =>
                parser.decode[T](jsonStr) match {
                  case Right(value) => Sync[F].pure(value)
                  case Left(err) =>
                    Sync[F].raiseError(
                      new RuntimeException(
                        s"Failed to decode GCS object $bucket/$path: ${err.getMessage}",
                        err
                      )
                    )
                }
              }

            RetryWrapper.withRetry[F, T](
              config.retry,
              classifier,
              "gcs-read"
            )(operation)
          }

          def readBytes(bucket: String, path: String): F[Array[Byte]] = {
            val operation: F[Array[Byte]] = Sync[F].blocking {
              // TODO: val blob = storage.get(bucket, path)
              // TODO: blob.getContent()
              ???
            }

            RetryWrapper.withRetry[F, Array[Byte]](
              config.retry,
              classifier,
              "gcs-read-bytes"
            )(operation)
          }

          def writeJson[T: io.circe.Encoder](
              bucket: String,
              path: String,
              value: T
          ): F[Unit] = {
            val operation: F[Unit] = Sync[F].blocking {
              import io.circe.syntax.*
              val json = value.asJson.noSpaces
              val bytes = json.getBytes(java.nio.charset.StandardCharsets.UTF_8)
              // TODO: val blobId = com.google.cloud.storage.BlobId.of(bucket, path)
              // TODO: val blobInfo = com.google.cloud.storage.BlobInfo.newBuilder(blobId)
              //         .setContentType("application/json")
              //         .build()
              // TODO: storage.create(blobInfo, bytes)
              ???
            }

            RetryWrapper.withRetry[F, Unit](
              config.retry,
              classifier,
              "gcs-write"
            )(operation)
          }

          def listVersioned(
              bucket: String,
              prefix: String,
              version: String
          ): F[List[String]] =
            listAll(bucket, prefix).map { paths =>
              paths.filter(_.contains(s"-$version."))
            }

          def listAll(bucket: String, prefix: String): F[List[String]] = {
            val operation: F[List[String]] = Sync[F].blocking {
              // TODO:
              // import com.google.cloud.storage.Storage.BlobListOption
              // val blobs = storage.list(bucket, BlobListOption.prefix(prefix))
              // blobs.iterateAll().asScala.map(_.getName).toList
              ???
            }

            RetryWrapper.withRetry[F, List[String]](
              config.retry,
              classifier,
              "gcs-list"
            )(operation)
          }
        }
      }
}
