package comp.bio.aging.crispr

import com.holdenkarau.spark.testing.SharedSparkContext
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.bdgenomics.adam.rdd.ADAMContext._


class GuidomeTest extends SparkTestBase{

  val dnas: Seq[String] = Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT",
    "CAGCTGATCTCCAGATATGACCATGGGTTT",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA")

  val merged = dnas.reduce(_ + _)


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
      cas9.guidome(fragments, includePam = true).rdd
        .map(fragment => (fragment.getFragmentStartPosition, fragment.getFragmentSequence))
        .collect()
        .toSet shouldEqual rightResults


      cas9.guidome(fragments, includePam = true, flankAdjacent = true).rdd
        .map(fragment=>(fragment.getFragmentStartPosition, fragment.getFragmentSequence))
        .collect()
        .toSet shouldEqual rightResults
    }

    "Cut from guidome in a right place" in {
      val cas9 = new Cas9ADAM
      val dic = new SequenceDictionary(Vector(SequenceRecord("test", merged.length)))
      val rdd = sc.parallelize(dnas2fragments(dnas))
      val fragments = new NucleotideContigFragmentRDD(rdd, dic)
      val guides: NucleotideContigFragmentRDD = cas9.guidome(fragments, includePam = true)
      val rightCuts = Set(21L, 22L, 50L, 51L, 81L)
      val cuts: Set[Long] = cas9.cutome(guides).map{ case (_, (cut, _)) => cut.start}.collect().toSet
      cuts shouldEqual rightCuts
    }

  }
}
