package comp.bio.aging.crispr

import scala.annotation.tailrec
import scala.collection.immutable.{List, Nil}



trait CRISPR
{
  def pam: String

  def forwardCut: Int

  def reverseCut: Int

  def compare(what: String, where: String, start: Int) = what.indices
    .forall{ i=> basesEqual(what(i).toUpper, where(start + i).toUpper) }

  def basesEqual(base1: Char, base2: Char): Boolean = (base1, base2) match {
    case (a, b) if a == b => true
    case ('N', _) => true
    case ('V', b) => b != 'T'
    //case ('H', b) => b != 'G'
    //case ('D', b) => b != 'C'
    //case ('B', b) => b != 'A'
    case ('W', b) => b == 'A' || b == 'T' //weak bonds
    case ('S', b) => b == 'G' || b == 'C' //strong bonds
    //case ('M', b) => b == 'A' || b == 'C' //amino
    //case ('K', b) => b == 'G' || b == 'T' //keto
    case ('Y', b) => b == 'T' || b == 'C' //pyrimidine
    case ('R', b) => b == 'G' || b == 'A' //purine
    case _ => false
  }

  @tailrec final def matchSeq(what: String)(where: String, start: Int, after: Int): Int =
    if(start + what.length + after > where.length) -1
    else
      if(compare(what, where, start)) start else matchSeq(what)(where, start + 1, after)


  def cuts(pams: List[Int]): List[(Int, Int)] = pams.map{p=>
    (
      p + (if (forwardCut < 0) forwardCut else forwardCut + pam.length) ,
      p + (if (reverseCut < 0) reverseCut else reverseCut + pam.length)
    )
  }


  @tailrec final def searchesOf(where: String, what: String, start: Int = 0,
                                before: Int = 0,
                                after: Int = 0, acc: List[Int] = Nil): List[Int] = {
    if(start < before)
      searchesOf(where, what, before, before, after, acc)
    else
      if( start + what.length + after > where.length ) acc.reverse
    else
        matchSeq(what)(where, start, after) match {
          case -1 => acc.reverse
          case index =>
            searchesOf(where, what, index + 1, before, after, index :: acc)
        }
  }

  /*
  @tailrec final def matchesOf(where: String, start: Int = 0, acc: List[Int] = Nil): List[Int] = matchSeq(pam)(where, start) match {
    case -1 => acc.reverse
    case index => matchesOf(where, start + pam.length, index :: acc)
  }
  */

    //def pams(where: String): List[Int] = matchesOf(where)
}