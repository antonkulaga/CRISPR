package comp.bio.aging.crispr


import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by antonkulaga on 3/23/17.
  */
class HomologyArmsTest extends SparkTestBase {

  lazy val armLen = 1500
  val left = ("A" * armLen).sliding(30, 30).toVector
  val right = ("T" * armLen).sliding(30, 30).toVector


  val dnas: Seq[String] = left ++ Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT",
    "CAGCTGATCTCCAGATTTGACCATGGGTTT",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA") ++ right

  val merged: String = dnas.reduce(_ + _)

  lazy val dic = new SequenceDictionary(Vector(SequenceRecord("test", merged.length)))

  lazy val cas9 = new Cas9ADAM

  lazy val rightCuts: Set[Long] = Set(21L, 22L, 50L, 51L, 81L).map(_ + armLen)


  def prepareFragments(): NucleotideContigFragmentRDD = {
    val rdd = sc.parallelize(dnas2fragments(dnas))
    new NucleotideContigFragmentRDD(rdd, dic)
  }

  "check list of restriction enzymes" in {
    val str = "GCGGCCGC" +"ATGC"+ "AGATCT" + "CGAT" + "CTCGAG" + "CG" +"GGCGCGCC" + "TCGCGA" + "ACGCGT"
    val enzymes = Set("NotI", "BglII", "XhoI", "AscI", "MluI", "NruI")
    val found: Set[String] = RestrictionEnzymes.find(str)
    found.intersect(enzymes).shouldEqual(enzymes)
  }


  "make right homology arms for all cuts" in {
    val fragments = prepareFragments()
    val guides = cas9.guidome(fragments, includePam = true)
    val forwardCuts: Set[Long] = cas9.cutome(guides).map(_._2._1.start).collect().toSet
    forwardCuts shouldEqual rightCuts
  }


  "compute homology arms for cas9" in {
    val fragments = prepareFragments()
    val guides = cas9.guidome(fragments, includePam = true)
    val cuts: RDD[(String, (ReferencePosition, ReferencePosition))] = cas9.cutome(guides)
    val arms: collection.Map[String, KnockIn] = cas9.arms(fragments, cuts, 1500L, 1500L).collectAsMap()
    arms.size shouldEqual 5
  }

}
