package comp.bio.aging.crispr.services

import io.circe.parser._

import scala.util.{Failure, Try}
/*
/**
  * runs azimuth scorer
  * right now it is packed in docker container
  * docker run -p 5000:5000  quay.io/comp-bio-aging/azimuth-server
  */
object AzimuthServer {

  def onTarget(guides: List[String]): Try[Map[String, Double]] = {
    assert(guides.forall(_.length>=30), "current score function also looks into context, so 30+ nucleotides required")

    import io.circe.{JsonObject, _}
    import io.circe.syntax._

    import scalaj.http._
    val data: JsonObject = JsonObject.fromMap(
      Map( "sequences" -> guides.asJson ) )

    val result = Http("http://localhost:5000/")
      .header("Content-Type", "application/json")
      .postData(data.asJson.spaces2)
    parse(result.asString.body) match {
      case Left(p: ParsingFailure) => Failure(p)
      case Right(json) =>
        Try {
          val cursor = json.hcursor.downField("scores")
          val mp = cursor.focus.flatMap(_.asObject).get.toMap
          mp.mapValues(_.asNumber.get.toDouble)
        }
    }
  }



}
*/
