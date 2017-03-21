package comp.bio.aging.crispr

import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD


class Cas9ADAM extends Cas9 with CRISPRADAM{
  /*
  def guidome(contigFragmentRDD: NucleotideContigFragmentRDD, guideRNA: String = "") = {
      contigFragmentRDD.transform{ rdd =>
        rdd.map(contig=>matchesOf(contig.getFragmentSequence))
      }
  }
  */

}