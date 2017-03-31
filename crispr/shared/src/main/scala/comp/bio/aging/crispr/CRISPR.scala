package comp.bio.aging.crispr

import scala.annotation.tailrec
import scala.collection.immutable.{List, Nil}

trait CRISPR
{
  def pam: String

  def guideEnd: Int

  def forwardCut: Int

  def reverseCut: Int

  def crRNA: String

  /**
    * Searchs for the guides to match
    * @param where string in which we search for guides
    * @param includePam
    * @param addBefore
    * @param addAfter
    * @return
    */
  def guideSearch(where: String, includePam: Boolean = false, addBefore: Int = 0, addAfter: Int = 0): List[(Long, String)] = {
    val (before, after) = if(guideEnd < 0)
        (addBefore - guideEnd, addAfter)
    else
        (addBefore, guideEnd + addAfter)

    val pamStartAdd = if(guideEnd >= 0 && !includePam) pam.length else 0
    val pamEndAdd =  if(guideEnd < 0  && includePam) pam.length else 0

    searchesOf(where, pam, 0, before , after).map{ i=>
        val start = i - before + pamStartAdd
        val end = i + after + pamEndAdd
        (start: Long , where.substring(start, end) )
    }

  }

  /**
    * Here we search for pam sequences
    * @param where
    * @return
    */
  def pamSearch(where: String): List[Int] = if(guideEnd < 0) searchesOf(where, pam, 0, guideEnd, 0) else searchesOf(where, pam, 0, 0, Math.abs(guideEnd))


  def cuts(pams: List[Int]): List[(Int, Int)] = pams.map{p=>
    (
      p + (if (forwardCut < 0) forwardCut else forwardCut + pam.length) ,
      p + (if (reverseCut < 0) reverseCut else reverseCut + pam.length)
    )
  }

  def cutsGuided(guided: Seq[(Long, String)]): Seq[(String, (Long, Long))] = guided.map{ case (index, guide) =>
    val p = index - guideEnd
    guide -> (
      p + (if (forwardCut < 0) forwardCut else forwardCut + pam.length) ,
      p + (if (reverseCut < 0) reverseCut else reverseCut + pam.length)
    )
  }



  def cutsGuided(pams: List[Int], where: String): List[(String, (Long, Long))] = pams.map{p=>
    if(guideEnd <0 && p + guideEnd < 0) println(s"MENSHE NULYA, $where and pam $p and g $guideEnd")
    //if(guideEnd >0 && p + guideEnd < 0) println(s"MENSHE NULYA, $where and pam $p and g $guideEnd")
    val guide = if(guideEnd < 0) where.substring(p + guideEnd, p) else where.substring(p + pam.length, p + pam.length + guideEnd)
      guide -> (
        p + (if (forwardCut < 0) forwardCut else forwardCut + pam.length): Long ,
        p + (if (reverseCut < 0) reverseCut else reverseCut + pam.length): Long
      )
  }

  def compare(what: String, where: String, start: Int): Boolean = what.indices
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

}