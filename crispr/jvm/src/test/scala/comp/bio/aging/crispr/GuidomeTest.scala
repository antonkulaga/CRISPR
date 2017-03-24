package comp.bio.aging.crispr

import com.holdenkarau.spark.testing.SharedSparkContext
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.bdgenomics.adam.rdd.ADAMContext._


class GuidomeTest extends WordSpec with Matchers with BeforeAndAfterAll with SharedSparkContext {

  def sparkContext = sc

  val dnas: Seq[String] = Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT",
    "CAGCTGATCTCCAGATATGACCATGGGTTT",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA")

  val merged = dnas.reduce(_ + _)

  def contig() = {
    val c= new Contig()
    c.setContigName("test")
    c
  }

  protected def makeFragment(str: String, start: Long) = {
    NucleotideContigFragment.newBuilder()
      .setContig(contig())
      .setFragmentStartPosition(start)
      .setFragmentLength(str.length: Long)
      .setFragmentSequence(str)
      .setFragmentEndPosition(start + str.length)
      .build()
  }

  def dnas2fragments(dnas: Seq[String]): List[NucleotideContigFragment] = {
    val (_, frags) = dnas.foldLeft((0L, List.empty[NucleotideContigFragment]))
    {
      case ((start, acc), str) => (start + str.length, makeFragment(str, start)::acc)
    }
    frags.reverse
  }

  "CAS9" should {

    val rightResults: Set[(Long, String)] = Set(
      (4L, "CTGATCTCCAGATATGACCATGG"),
      (5L, "TGATCTCCAGATATGACCATGGG"),
      (33L, "CTGATCTCCAGATATGACCATGG"),
      (34L, "TGATCTCCAGATATGACCATGGG"),
      (64L, "AAGTTTGAGCCACAAACCCATGG")
    )

    "get right guides from merged string" in {
      val cas9 = new Cas9ADAM
      cas9.guideSearch(merged, true).toSet shouldEqual rightResults
      cas9.guideSearch(merged, false).toSet should not be rightResults
    }


    "get right guidome" in {
      val cas9 = new Cas9ADAM
      val dic = new SequenceDictionary(Vector(SequenceRecord("test", merged.length)))
      val rdd = sc.parallelize(dnas2fragments(dnas))
      val fragments = new NucleotideContigFragmentRDD(rdd, dic)
      //cas9.guideSearch(merged, true).toSet shouldEqual rightResults
      cas9.guidome(fragments, includePam = true).rdd
        .map(fragment => (fragment.getFragmentStartPosition, fragment.getFragmentSequence))
        .collect()
        .toSet shouldEqual rightResults


      cas9.guidome(fragments, includePam = true, flankAdjacent = true).rdd
        .map(fragment=>(fragment.getFragmentStartPosition, fragment.getFragmentSequence))
        .collect()
        .toSet shouldEqual rightResults

    }
  }
}
