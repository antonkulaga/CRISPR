package comp.bio.aging.crispr.services

import fr.hmil.roshttp.HttpRequest
import fr.hmil.roshttp.body.URLEncodedBody
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import monix.execution.Scheduler.Implicits.global
import sun.util.resources.cldr.yo.CalendarData_yo_NG

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


object CindelData {
  import io.circe.generic.semiauto._
  implicit val decoder: Decoder[CindelData] = deriveDecoder[CindelData]
  implicit val encoder: Encoder[CindelData] = deriveEncoder[CindelData]
  //implicit val decoderList: Decoder[Array[CindelData]] = deriveDecoder[Array[CindelData]]

}


case class CindelData(value: Double, GCContent: String, FreeEnergy: Double, SumOfWeights: Double, Intercept: Double)

/**
  * Cpf1 indel evaluation
  * @param base
  * @param script
  */
class Cindel(val base: String = "http://big.hanyang.ac.kr", script: String ="/engine/cindel_text_fasta-3.0.php") {


  lazy val post: String = base + script

  def sendFasta(namedGuides: List[(String, String)]): Future[String] = {
    val seq = namedGuides.foldLeft(""){ case (acc, (name, guide)) => acc +  s">$name\n$guide\n"}
    val urlEncodedData = URLEncodedBody(
      "hasGCFilter" -> "1",
      "theSeqText" -> seq
    )
    HttpRequest(post).post(urlEncodedData).map(resp => resp.body.lines.mkString)
  }

  def getScoresString(namedGuides: List[(String, String)]): Future[String] = {
    sendFasta(namedGuides)
      .flatMap(res=>HttpRequest(base+res)
        .withHeader("Content-Type", s"application/json; charset=utf-8")
        .send()
        .map(b=> b.body))
  }

  def getScoresJson(namedGuides: List[(String, String)]): Future[Either[ParsingFailure, Json]] =
    getScoresString(namedGuides).map(parse)

  def getScores(namedGuides: List[(String, String)]): Future[List[CindelData]] = {
    getScoresJson(namedGuides)
      .flatMap{j =>
        val score = j.right.map {
          data =>
            data.hcursor.downField("data").focus.get.as[List[CindelData]]
        } match {
          case Left(failure) => Failure(new Exception(failure.message))
          case Right(Left(failure)) => Failure(new Exception(failure.message))
          case Right(Right(result)) => Success(result)
        }
        Future.fromTry(score)
      }
  }


  def getScore(name: String, guide: String): Future[CindelData] =
    getScores(List((name, guide))).map(_.head)
}