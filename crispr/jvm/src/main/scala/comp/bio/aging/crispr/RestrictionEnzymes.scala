package comp.bio.aging.crispr

import java.io.File

import scala.collection.immutable
import scala.io.Source

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

  def notIncludesEnzymes(where: String, what: Set[String]): Boolean = {
    val values = enzymes2seqMap.filterKeys(what.contains).values.toSet
    notIncludesSites(where, values)
  }

  def notIncludesSites(where: String, what: Set[String]): Boolean = {
    what.forall(s=> !where.contains(s))
  }

}
