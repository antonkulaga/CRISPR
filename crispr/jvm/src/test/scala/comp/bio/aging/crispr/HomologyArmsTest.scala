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


  val initialDNAs: Seq[String] =  Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT",
    "CAGCTGATCTCCAGATTTGACCATGGGTTT",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA")



  lazy val dic = new SequenceDictionary(Vector(SequenceRecord("test", initialDNAs.length + armLen * 2)))

  lazy val cas9 = new Cas9ADAM

  lazy val cpf1 = new Cpf1ADAM

  lazy val rightCuts: Set[Long] = Set(21L, 22L, 50L, 51L, 81L).map(_ + armLen)


  def prepareFragments(dnas: Seq[String]): NucleotideContigFragmentRDD = {
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
    val left = ("A" * armLen).sliding(30, 30).toVector
    val right = ("T" * armLen).sliding(30, 30).toVector
    val dnas = left ++ initialDNAs ++ right

    val fragments = prepareFragments(dnas)
    val guides = cas9.guidome(fragments, includePam = true)
    val forwardCuts: Set[Long] = cas9.cutome(guides).map(_.top.start).collect().toSet
    forwardCuts shouldEqual rightCuts
  }


  "compute homology arms for cas9" in {

    val left = ("A" * armLen).sliding(30, 30).toVector
    val right = ("T" * armLen).sliding(30, 30).toVector

    val fragments1 = prepareFragments(left ++ initialDNAs ++ right)
    val guides = cas9.guidome(fragments1, includePam = true)
    val cuts = cas9.cutome(guides)
    val arms = cas9.arms(fragments1, cuts, 1500L, 1500L).collect()
    arms.size shouldEqual 5
    arms.forall{case KnockIn(_,leftSeq, leftRegion, rightSeq, rightRegion)=>
      leftSeq.length == 1500 && rightSeq.length == 1500 &&
      leftRegion.length == 1500L && rightRegion.length == 1500L
    }

    val a = Set("NotI", "BglII", "XhoI")
    val b = Set("AscI", "MluI", "NruI")
    val c = Set("NotI", "NruI")
    val enzymes = a ++ b
    val NotI = "GCGGCCGC"
    val NruI = "TCGCGA"
    import comp.bio.aging.playground.extensions.stringSeqExtensions._
    val MluIRev = "ACGCGT".complement.reverse
    val left2 =  s"TCCAGATATGACCATGGGT${NotI}T" +: s"CCAGAAG${MluIRev}CCACA${NruI}TGGTCA" +: left.tail.tail
    val right2 = right.tail :+ s"C${NruI}CAGAAGTTTGAGCCCATGGTCA"
    val fragments2 = prepareFragments(left2 ++ initialDNAs ++ right2)

    val guides2 = cas9.guidome(fragments2, includePam = true)
    val cuts2 = cas9.cutome(guides2)
    val arms2 = cas9.arms(fragments2, cuts2, 1500L, 1500L).collect()
    arms2.size shouldEqual 5
    arms2.forall{case KnockIn(_,leftSeq, leftRegion, rightSeq, rightRegion)=>
      leftSeq.length == 1500 && rightSeq.length == 1500 &&
        leftRegion.length == 1500L && rightRegion.length == 1500L
    }

    val armsNotI = cas9.arms(fragments2, cuts2, 1500L, 1500L, RestrictionEnzymes.enzymesSites(Set("NotI"), reverseComplement = true)).collect()
    val withNotI = arms2.toList.exists(k=>k.leftArm.contains("GCGGCCGC") || k.rightArm.contains("GCGGCCGC") )
    armsNotI.size shouldEqual 4
    armsNotI.forall{case KnockIn(_,leftSeq, leftRegion, rightSeq, rightRegion)=>
      leftSeq.length == 1500 && rightSeq.length == 1500 &&
        leftRegion.length == 1500L && rightRegion.length == 1500L
    }

    val armsMluI = cas9.arms(fragments2, cuts2, 1500L, 1500L, RestrictionEnzymes.enzymesSites(Set("MluI"), true)).collect()
    armsMluI.size shouldEqual 3
    armsMluI.forall{case KnockIn(_,leftSeq, leftRegion, rightSeq, rightRegion)=>
      leftSeq.length == 1500 && rightSeq.length == 1500 &&
        leftRegion.length == 1500L && rightRegion.length == 1500L
    }

    val armsNru = cas9.arms(fragments2, cuts2, 1500L, 1500L, RestrictionEnzymes.enzymesSites(Set("NruI"), true)).collect()
    armsNru.size shouldEqual 2
    armsNru.forall{case KnockIn(_,leftSeq, leftRegion, rightSeq, rightRegion)=>
      leftSeq.length == 1500 && rightSeq.length == 1500 &&
        leftRegion.length == 1500L && rightRegion.length == 1500L
    }
    //TODO: more tests on restricton-digestion
  }


  val initialCpf1DNAs: Vector[String] = Vector(
    "TTTAAACTACGAGCGCTTTGTGCCCCG",
    "TTTAATCCTTGGTGGTGAAGTTGGCTA",
    "TTTACACCGAGTGGTGGGTACGGTGGT",
    "TTTAAACCTCGTCCGCCACGACTACCG"
  )

  "compute homology arms for cpf1" in {
    val left = ("A" * armLen).sliding(30, 30).toVector
    val right = ("T" * armLen).sliding(30, 30).toVector
    val dnas = left ++ initialCpf1DNAs ++ right
    val fragments = prepareFragments(dnas)
    val guides = cpf1.guidome(fragments, includePam = true)
    val cuts: RDD[CutDS] = cpf1.cutome(guides)
    val top: RDD[Long] = cuts.map(_.top.pos)
    top.collect().toList shouldEqual List(1500L + 4 + 18, 1500L + 4 + 18 + 27, 1500L + 4 + 18 + 27 *2, 1500L + 4 + 18 + 27 *3)

    val arms1 = cpf1.arms(fragments, cuts, 1500L, 1500L, allowOverlap = false).collect()
    arms1.size shouldEqual 4
    arms1.forall{case KnockIn(_,leftSeq, leftRegion, rightSeq, rightRegion)=>
      leftSeq.length == 1500 && rightSeq.length == 1500 &&
        leftRegion.length == 1500L && rightRegion.length == 1500L
    }
    val arms2 = cpf1.arms(fragments, cuts, 1500L, 1500L, allowOverlap = true).collect()
    arms2.size shouldEqual 4
    arms2.forall{case KnockIn(_,leftSeq, leftRegion, rightSeq, rightRegion)=>
      leftSeq.length == 1500 + 5 && rightSeq.length == 1500 + 5 &&
        leftRegion.length == 1500L + 5 && rightRegion.length == 1500L + 5
    }
  }

}