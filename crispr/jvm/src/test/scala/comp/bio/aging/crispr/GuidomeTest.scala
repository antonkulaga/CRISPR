package comp.bio.aging.crispr

import com.holdenkarau.spark.testing.SharedSparkContext
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.bdgenomics.adam.rdd.ADAMContext._


class GuidomeTest extends SparkTestBase{

  "CAS9ADAM" should {


    val dnas: Seq[String] = Vector(
      "ACAGCTGATCTCCAGATATGACCATGGGTT",
      "CAGCTGATCTCCAGATATGACCATGGGTTT",
      "CCAGAAGTTTGAGCCACAAACCCATGGTCA")

    val merged = dnas.reduce(_ + _)


    val rightResults: Set[(Long, String)] = Set(
      (4L, "CTGATCTCCAGATATGACCATGG"),
      (5L, "TGATCTCCAGATATGACCATGGG"),
      (33L, "CTGATCTCCAGATATGACCATGG"),
      (34L, "TGATCTCCAGATATGACCATGGG"),
      (64L, "AAGTTTGAGCCACAAACCCATGG")
    )

    "get right guides from merged string" in {
      val cas9 = new Cas9ADAM
      cas9.guideSearchIn(merged, true).toSet shouldEqual rightResults
      cas9.guideSearchIn(merged, false).toSet should not be rightResults
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
      val cuts: Set[Long] = cas9.cutome(guides).map{ cut =>
        cut.top.start }.collect().toSet
      cuts shouldEqual rightCuts
    }

  }

  "Cpf1ADAM" should {


    val dnas: Seq[String] = Vector(
      "TTTAAACTACGAGCGCTTTGTGCCCCG",
      "TTTAATCCTTGGTGGTGAAGTTGGCTA",
      "TTTACACCGAGTGGTGGGTACGGTGGT",
      "TTTAAACCTCGTCCGCCACGACTACCG"
    )

    val merged = dnas.reduce(_ + _)


    val rightResults: Set[(Long, String)] = Set(
      (0L , "TTTAAACTACGAGCGCTTTGTGCCCCG"),
      (16L ,"TTTGTGCCCCGTTTAATCCTTGGTGGT"),
      (27L, "TTTAATCCTTGGTGGTGAAGTTGGCTA"),
      (54L, "TTTACACCGAGTGGTGGGTACGGTGGT"),
      (81L, "TTTAAACCTCGTCCGCCACGACTACCG")
      )

    "get right guides from merged string" in {
      val cpf1 = new Cpf1ADAM
      cpf1.guideSearchIn(merged, true).toSet shouldEqual rightResults
      cpf1.guideSearchIn(merged, false).toSet should not be rightResults
    }


    "get right guidome" in {
      val cpf1 = new Cpf1ADAM
      val dic = new SequenceDictionary(Vector(SequenceRecord("test", merged.length)))
      val rdd = sc.parallelize(dnas2fragments(dnas))
      val fragments = new NucleotideContigFragmentRDD(rdd, dic)
      cpf1.guidome(fragments, includePam = true, flankAdjacent = true).rdd
        .map(fragment => (fragment.getFragmentStartPosition, fragment.getFragmentSequence))
        .collect()
        .toSet shouldEqual rightResults


      cpf1.guidome(fragments, includePam = true, flankAdjacent = true).rdd
        .map(fragment=>(fragment.getFragmentStartPosition, fragment.getFragmentSequence))
        .collect()
        .toSet shouldEqual rightResults
    }

    "Cut from guidome in a right place" in {
      val cpf1 = new Cpf1ADAM
      val dic = new SequenceDictionary(Vector(SequenceRecord("test", merged.length)))
      val rdd = sc.parallelize(dnas2fragments(dnas))
      val fragments = new NucleotideContigFragmentRDD(rdd, dic)
      val guides: NucleotideContigFragmentRDD = cpf1.guidome(fragments, includePam = true, flankAdjacent = true)
      val rightForwardCuts = Set(0L, 16L, 27L, 54L, 81L).map(_ + 18L + 4L)
      val rightBackwardCuts = Set(0L, 16L, 27L, 54L, 81L).map(_ + 23L + 4L)
      //println("CPF1 GUIDES =")
      //pprint.pprintln(guides.rdd.collect())
      val cutome = cpf1.cutome(guides)
      //println("CPF1 CUTS =")
      //pprint.pprintln(cutome.collect)

      cutome.map{ cut => cut.top.start }.collect().toSet shouldEqual rightForwardCuts
      cutome.map{ cut => cut.bottom.start }.collect().toSet shouldEqual rightBackwardCuts
    }
  }

}