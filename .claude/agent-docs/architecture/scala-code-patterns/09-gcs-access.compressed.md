<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/scala-code-patterns/09-gcs-access.md -->

## 9. GCS Access

**Pattern**: Google Cloud Storage Java SDK wrapped in `Sync[F]`. Used for loading prompt configuration (instruction blocks and profiles).

```scala
import com.google.cloud.storage.{Storage, StorageOptions, BlobId}
import cats.effect.Sync

trait GcsClient[F[_]] {
  def readObject(bucket: String, path: String): F[String]
  def listObjects(bucket: String, prefix: String): F[List[String]]
}

class GcsClientImpl[F[_]: Sync] extends GcsClient[F] {

  private val storage: F[Storage] = Sync[F].delay {
    StorageOptions.getDefaultInstance.getService
  }

  override def readObject(bucket: String, path: String): F[String] =
    storage.flatMap { gcs =>
      Sync[F].blocking {
        val blob = gcs.get(BlobId.of(bucket, path))
        if (blob == null) throw GcsReadFailed(bucket, path,
          new NoSuchElementException(s"Object not found: gs://$bucket/$path"))
        new String(blob.getContent(), java.nio.charset.StandardCharsets.UTF_8)
      }
    }

  override def listObjects(bucket: String, prefix: String): F[List[String]] =
    storage.flatMap { gcs =>
      Sync[F].blocking {
        import scala.jdk.CollectionConverters.*
        gcs.list(bucket, Storage.BlobListOption.prefix(prefix))
          .iterateAll()
          .asScala
          .map(_.getName)
          .toList
      }
    }
}
```

### Rules
- GCS SDK calls wrapped in `Sync[F].blocking { ... }`
- GCS used **only** for prompt configuration — not general data storage
- Prompt fragments read at pipeline startup and cached for duration of run
- Use `GcsReadFailed` error for missing objects