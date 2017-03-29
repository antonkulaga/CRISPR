package comp.bio.aging.crispr.services

import fr.hmil.roshttp.HttpRequest
import fr.hmil.roshttp.body.URLEncodedBody
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser._
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


object CindelData {
  implicit val decoder: Decoder[CindelData] = deriveDecoder[CindelData]
  implicit val encoder: Encoder[CindelData] = deriveEncoder[CindelData]
}


case class CindelData(value: Double, GCContent: String, FreeEnergy: Double, SumOfWeights: Double, Intercept: Double)

/**
  * Cpf1 indel evaluation
  * @param base
  * @param script
  */
class Cindel(val base: String = "http://big.hanyang.ac.kr", script: String ="/engine/cindel_text_fasta-3.0.php") {

  protected implicit def tryToFuture[T](t: Try[T]): Future[T] = {
    t match{
      case Success(s) => Future.successful(s)
      case Failure(ex) => Future.failed(ex)
    }
  }

  lazy val post: String = base + script

  def sendFasta(name: String, guide: String): Future[String] = {
    val seq = s"""
                |>$name
                |$guide
              """.stripMargin

    val urlEncodedData = URLEncodedBody(
      "hasGCFilter" -> "1",
      "theSeqText" -> seq
    )
    HttpRequest(post).post(urlEncodedData).map(resp => resp.body.lines.mkString)
  }

  def getScoreJson(name: String, guide: String): Future[Either[ParsingFailure, Json]] = {
    sendFasta(name, guide)
      .flatMap(res=>HttpRequest(base+res)
        .send()
        .map(b=> parse(b.body)) )
  }

  def getScore(name: String, guide: String): Future[CindelData] = {
    getScoreJson(name, guide)
      .flatMap{j =>
        val score: Try[CindelData] = j.right.map(
          data=>
            data.hcursor.downField("data").downArray.first.as[CindelData](CindelData.decoder)
        ) match {
          case Left(failure) => Failure(new Exception(failure.message))
          case Right(Left(failure)) => Failure(new Exception(failure.message))
          case Right(Right(result)) => Success(result)
        }
        Future.fromTry(score)
      }
  }
}