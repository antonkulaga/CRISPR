package comp.bio.aging.crispr

import comp.bio.aging.crispr.services.GenomeCRISPR

import scala.concurrent.Await
import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import java.io.{File => JFile}

import scala.reflect.io.File

object GenomeCRISPRTester extends scala.App {

  val chr = "Y"

  val results: String = Await.result(
    GenomeCRISPR.sgRNA.byChromosomeRequest(chr).map(_.body)
    , 360 seconds)

  val f = File("/home/antonkulaga/Documents/data2.txt")
  f.writeAll(results)

  println("=====RESULTS=====")
  val res = GenomeCRISPR.sgRNA.toCRISPRScreenResult(results)
  for(r <- res)
  {
    println(r)
  }

}
