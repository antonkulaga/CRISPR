package comp.bio.aging.crispr

import com.holdenkarau.spark.testing.SharedSparkContext
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

/**
  * Created by antonkulaga on 3/24/17.
  */
class SparkTestBase extends WordSpec with Matchers with BeforeAndAfterAll with SharedSparkContext{

  def sparkContext = sc

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
}
