package comp.bio.aging.crispr

import crispr.Cyndel

import scala.concurrent.Await
import scala.concurrent.duration._


object Main extends App{

  val c = new Cyndel()
  val result= c.getScore("hello", "TTTACAGTGACGTCGGTTAGGACACTG")
  println( Await.result(result, 6 seconds))
}
