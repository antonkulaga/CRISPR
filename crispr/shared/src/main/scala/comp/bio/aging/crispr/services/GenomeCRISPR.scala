package comp.bio.aging.crispr.services
/*
import fr.hmil.roshttp.HttpRequest
import fr.hmil.roshttp.body.JSONBody.JSONObject
import io.circe.{Decoder, Encoder, Json, Parser, _}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import fr.hmil.roshttp.body.Implicits._
import fr.hmil.roshttp.body.JSONBody._
import fr.hmil.roshttp.HttpRequest
import fr.hmil.roshttp.body.{JSONBody, MultiPartBody, URLEncodedBody}
import fr.hmil.roshttp.response.SimpleHttpResponse
import io.circe.generic.semiauto._
import io.circe.parser._
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object GenomeCRISPR {
  lazy val experiments = new GenomeCRISPRExperiment()
  lazy val sgRNA = new GenomeCRISPRsgRNA()
}

class GenomeCRISPRExperiment(base: String = "http://genomecrispr.dkfz.de/api/experiments/") {

  protected def toCRISPRExperiment(response: SimpleHttpResponse): Future[List[CRISPRExperimentResult]] = {
    val parseResult = parse(response.body).right.map(json=>json.as[List[CRISPRExperimentResult]]) match {
      case Left(failure) => Failure(new Exception(failure.message))
      case Right(Left(failure)) => Failure(new Exception(failure.message))
      case Right(Right(result)) => Success(result)
    }
    Future.fromTry(parseResult)
  }

}
object CRISPRExperimentResult {
  implicit val decoder: Decoder[CRISPRExperimentResult] = deriveDecoder[CRISPRExperimentResult]
  implicit val encoder: Encoder[CRISPRExperimentResult] = deriveEncoder[CRISPRExperimentResult]
}


case class CRISPRExperimentResult(
                                   pubmed:	String, // PubMed ID
                                   title:	String, // Title of publication
                                   `abstract`:	String, // Abstract of publication
                                   authors:	Array[String], // An array containing the author names
                                   conditions:	Json // An object containing information about experiments performed in cell lines
                                 )

object CRISPRscreenResult {
  implicit val decoder: Decoder[CRISPRscreenResult] = deriveDecoder[CRISPRscreenResult]
  implicit val encoder: Encoder[CRISPRscreenResult] = deriveEncoder[CRISPRscreenResult]
}



case class CRISPRscreenResult(name: String, //Name of sgRNA
                              genetargets: String, //Gene symbol::ENSEMBL Gene ID
                              log2fc: Long,
                              sequence: String, //including PAM
                              chr: String,
                              start: String,
                              end: String,
                              strand: String,
                              ensg: String, //ENSEMBL Gene ID
                              symbol: String, //Gene symbol
                              pubmed: String,
                              cellline: String, //Cell line
                              condition: String,
                              score: String, //Score of the target gene
                              scoredist: String, //Distribution of scores in the experiment
                              hit: String //If target gene was a hit in the experiment
                                 )


class GenomeCRISPRsgRNA(base: String = "http://genomecrispr.dkfz.de/api/sgrnas/", timeout: FiniteDuration = 6 minutes) {

  def toCRISPRScreenResult(str: String): Future[List[CRISPRscreenResult]] = {
    val parseResult = parse(str).right.map(json=>json.as[List[CRISPRscreenResult]]) match {
      case Left(failure) => Failure(new Exception(failure.message))
      case Right(Left(failure)) => Failure(new Exception(failure.message))
      case Right(Right(result)) => Success(result)
    }
    Future.fromTry(parseResult)
  }

  def toCRISPRScreenResult(response: SimpleHttpResponse): Future[List[CRISPRscreenResult]] = {
    toCRISPRScreenResult(response.body)
  }

  def query(json: JSONObject): Future[List[CRISPRscreenResult]] = HttpRequest(base + "custom").withTimeout(timeout).post(json).flatMap(toCRISPRScreenResult)

  def byCellLineRequest(name: String): Future[SimpleHttpResponse] = HttpRequest(base + "cellline").withTimeout(timeout).post(JSONObject("cellline" -> name))

  def byCellLine(name: String): Future[List[CRISPRscreenResult]] = byCellLineRequest(name).flatMap(toCRISPRScreenResult)

  def byChromosomeRequest(name: String): Future[SimpleHttpResponse] = HttpRequest(base + "chr").withTimeout(timeout).post(JSONObject("chr" -> name))

  def byChromosome(name: String): Future[List[CRISPRscreenResult]] = byChromosomeRequest(name).flatMap(toCRISPRScreenResult)

}
*/