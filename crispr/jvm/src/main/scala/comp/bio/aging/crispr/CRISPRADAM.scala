package comp.bio.aging.crispr

import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import comp.bio.aging.playground.extensions._
import comp.bio.aging.playground.extensions.stringSeqExtensions._
import org.bdgenomics.formats.avro.{NucleotideContigFragment, Strand}

trait CRISPRADAM extends CRISPR with Serializable {


  /**
    * In order to include crispr's that are on the board of several regions
    * @param contigFragmentRDD
    * @return
    */
  def flank(contigFragmentRDD: NucleotideContigFragmentRDD, includePam: Boolean, additional: Int = 0): NucleotideContigFragmentRDD = {
    contigFragmentRDD.flankAdjacentFragments(Math.abs(this.guideEnd + (if(includePam) pam.length else 0) -1) + additional)
  }

  def cutome(contigFragmentRDD: NucleotideContigFragmentRDD) = {

  }


  /**
    * WARNING: not tested yet!
    * @param fragment
    * @param addBefore
    * @param addAfter
    * @return
    */
  protected def extractReverseGuideFragments(fragment: NucleotideContigFragment,
                                      includePam: Boolean,
                                      addBefore: Int = 0,
                                      addAfter: Int = 0): List[NucleotideContigFragment] = {
    val sequence = fragment.getFragmentSequence.complement.reverse
    guideSearch(sequence, includePam, addBefore, addAfter).map{
      case (index, seq)=>
        NucleotideContigFragment
          .newBuilder(fragment)
          .setFragmentStartPosition(fragment.getFragmentEndPosition - index)
          .setFragmentEndPosition(fragment.getFragmentEndPosition - (index + seq.length))
          .setFragmentNumber(null)
          .setFragmentSequence(seq)
          .setFragmentLength(seq.length: Long)
          .build()
    }
  }

  /**
    * Warning: works with the forward strand only right now
    * @param contigFragmentRDD genome fragments
    * @param addBefore
    * @param addAfter
    * @param flankAdjacent
    * @return
    */
  def guidome(contigFragmentRDD: NucleotideContigFragmentRDD,
              includePam: Boolean,
              addBefore: Int = 0,
              addAfter: Int = 0,
              flankAdjacent: Boolean= false): NucleotideContigFragmentRDD = {
    val fragments = if(flankAdjacent) {
      val extra = addBefore + addAfter
      flank(contigFragmentRDD, includePam, extra)
    } else contigFragmentRDD

    def extractForwardGuideFragments(fragment: NucleotideContigFragment,
                                     includePam: Boolean,
                                     addBefore: Int = 0,
                                     addAfter: Int = 0): List[NucleotideContigFragment] = {
      guideSearch(fragment.getFragmentSequence, includePam, addBefore, addAfter).map{
        case (index, seq)=>
          NucleotideContigFragment
            .newBuilder(fragment)
            .setFragmentStartPosition(fragment.getFragmentStartPosition + index)
            .setFragmentEndPosition(fragment.getFragmentStartPosition + (index + seq.length))
            .setFragmentNumber(null)
            .setFragmentSequence(seq)
            .setFragmentLength(seq.length: Long)
            .build()
      }
    }

    fragments.transform{
      rdd=>
        rdd.flatMap{ fragment=> extractForwardGuideFragments(fragment, includePam, addBefore, addAfter)}
    }
  }


}