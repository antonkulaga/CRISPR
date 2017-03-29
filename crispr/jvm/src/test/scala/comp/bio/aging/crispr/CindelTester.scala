package comp.bio.aging.crispr

import comp.bio.aging.crispr.services.Cindel

import scala.concurrent.Await
import scala.concurrent.duration._


object CindelTester extends App{

  val c = new Cindel()
  val result= c.getScore("hello", "TTTACAGTGACGTCGGTTAGGACACTG")
  println( Await.result(result, 6 seconds))
}
