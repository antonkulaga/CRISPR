package comp.bio.aging.crispr

//import comp.bio.aging.playground.trees.SimpleTree
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD

case object Rainbow extends Rainbow

trait Rainbow extends Serializable {

  /*
  def repeatsOf(frag: NucleotideContigFragmentRDD, str: String) = {
    frag.rdd.filter{  f => f.getSequence.contains(str) }.map(f=>f.getStart -> f.getSequence)
  }

  def findRepeats(guidomeNoPam: NucleotideContigFragmentRDD, minRepeats: Int) = {
    val tree = SimpleTree(guidomeNoPam)
    tree.repeatsOfMin(20, minRepeats)
  }
  */

  def countRepeats(guidomeNoPam: NucleotideContigFragmentRDD, moreThan: Int = 2): RDD[(String, Int)] = {
    val mp = guidomeNoPam.rdd.map(fr=>fr.getSequence->1)
    mp.reduceByKey(_ + _).filter(f=>f._2 >= moreThan)
  }
  
  def countRepeatsByContig(guidomeNoPam: NucleotideContigFragmentRDD, moreThan: Int = 5): RDD[((String, String), Int)] = {
    val mp = guidomeNoPam.rdd.map(fr=>(fr.getSequence, fr.getContigName)->1)
    mp.reduceByKey(_ + _).filter(f=>f._2 >= moreThan)
  }

}
