package comp.bio.aging.crispr

import java.io.File

import scala.collection.immutable
import scala.io.Source
import comp.bio.aging.playground.extensions.stringSeqExtensions._
object RestrictionEnzymes {

  lazy val enzymes2seq: Set[(String, String)] = Source
    .fromInputStream(getClass().getClassLoader().getResourceAsStream("restriction_enzymes.tsv"))
    .getLines().map(_.split(" ")).collect{ case iter if iter.size ==2 => (iter.head, iter.tail.head) }.toSet

  lazy val enzymes2seqMap: Map[String, String] = enzymes2seq.toMap

  lazy val seq2enzymes: Set[(String, String)] = enzymes2seq.map(_.swap)

  lazy val seq2enzymesMap: Map[String, Set[String]] = seq2enzymes.groupBy(_._1).mapValues(s=>s.map(_._2))

  def find(where: String): Set[String] = seq2enzymesMap.flatMap{
    case (str, enzymes)=> if(where.contains(str)) enzymes else Set.empty[String]
  }.toSet

  def enzymesSites(enzymes: Set[String], reverseComplement: Boolean = true): Set[String] = {
    val sites = enzymes2seqMap.filterKeys(enzymes.contains).values.toSet
    if(reverseComplement) sites ++ sites.map(s=>s.complement.reverse) else sites
  }

  def notIncludesEnzymes(where: String, enzymes: Set[String], reverseComplement: Boolean = true): Boolean = {
    val sites = enzymesSites(enzymes, reverseComplement)
    notIncludesSites(where, sites)
  }

  def notIncludesSites(where: String, what: Set[String]): Boolean = {
    what.forall(s=> !where.contains(s))
  }

}
