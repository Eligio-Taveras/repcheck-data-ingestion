package repcheck.members.committees.client

import cats.effect.Async
import cats.syntax.all._

import io.circe.Json
import io.circe.parser.parse

import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.members.committees.client.CdirPackageSelector.CdirPackageRef
import repcheck.members.committees.config.GovInfoConfig

/**
 * Fetches Congressional Directory committee-listing texts from the GovInfo API. For a congress it picks that congress's
 * directory edition, then downloads the House/Senate/Joint committee-listing granule txt renditions. HTML tags are
 * stripped so the plain directory text reaches [[CdirCommitteeListingParser]].
 */
class GovInfoCdirClient[F[_]: Async](
  client: Client[F],
  config: GovInfoConfig,
  logger: PipelineLogger[F],
) extends CdirCommitteeSource[F] {

  private val StepName = "committee-history-cdir"

  override def committeeListingTexts(congress: Int, runId: Long): F[List[String]] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName, entityId = Some(s"congress-$congress"))
    listPackages.flatMap { packages =>
      CdirPackageSelector.selectForCongress(packages, congress) match {
        case None =>
          logger.warn(logCtx, s"No CDIR package found for congress $congress").as(List.empty[String])
        case Some(pkg) =>
          for {
            _          <- logger.info(logCtx, s"Using CDIR package $pkg for congress $congress")
            granuleIds <- committeeGranuleIds(pkg)
            texts      <- granuleIds.traverse(g => fetchGranuleText(pkg, g))
          } yield texts.filter(_.nonEmpty)
      }
    }
  }

  private def listPackages: F[List[CdirPackageRef]] =
    getJson(s"${config.baseUrl}/collections/CDIR/2000-01-01T00:00:00Z?offset=0&pageSize=500").map { json =>
      jsonArray(json, "packages").flatMap { p =>
        for {
          id   <- p.hcursor.get[String]("packageId").toOption
          date <- p.hcursor.get[String]("dateIssued").toOption
        } yield CdirPackageRef(id, date)
      }
    }

  private def committeeGranuleIds(pkg: String): F[List[String]] =
    getJson(s"${config.baseUrl}/packages/$pkg/granules?offset=0&pageSize=1000").map { json =>
      jsonArray(json, "granules")
        .flatMap(_.hcursor.get[String]("granuleId").toOption)
        .filter(id => id.contains("HOUSECOMMITTEES") || id.contains("SENATECOMMITTEES"))
    }

  private def fetchGranuleText(pkg: String, granuleId: String): F[String] =
    getJson(s"${config.baseUrl}/packages/$pkg/granules/$granuleId/summary").flatMap { summary =>
      summary.hcursor.downField("download").get[String]("txtLink").toOption match {
        case Some(link) => getText(appendKey(link)).map(stripHtml)
        case None       => Async[F].pure("")
      }
    }

  private def jsonArray(json: Json, field: String): List[Json] =
    json.hcursor.downField(field).as[List[Json]].getOrElse(Nil)

  private def getJson(url: String): F[Json] =
    getText(appendKey(url)).flatMap(s => Async[F].fromEither(parse(s)))

  private def getText(url: String): F[String] =
    Async[F]
      .fromEither(Uri.fromString(url))
      .flatMap(uri => client.expect[String](Request[F](Method.GET, uri)))

  private def appendKey(url: String): String =
    if (url.contains("?")) s"$url&api_key=${config.apiKey}" else s"$url?api_key=${config.apiKey}"

  private def stripHtml(s: String): String =
    s.replaceAll("<[^>]*>", " ").replaceAll("&[a-zA-Z]+;", " ")

}
