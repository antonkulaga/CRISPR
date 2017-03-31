package comp.bio.aging.crispr

import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import comp.bio.aging.playground.extensions._
import comp.bio.aging.playground.extensions.stringSeqExtensions._
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, ReferenceRegion}
import org.bdgenomics.formats.avro.{NucleotideContigFragment, Strand}

import scala.collection.immutable.{List, Nil}

trait CRISPRADAM extends CRISPR with HomologyArms with Serializable {


  /**
    * In order to include crispr's that are on the board of several regions
    * @param contigFragmentRDD
    * @return
    */
  def flank(contigFragmentRDD: NucleotideContigFragmentRDD, includePam: Boolean, additional: Int = 0): NucleotideContigFragmentRDD = {
    contigFragmentRDD.flankAdjacentFragments(Math.abs(this.guideEnd + (if(includePam) pam.length else 0) -1) + additional)
  }

  def cutome(contigFragmentRDD: NucleotideContigFragmentRDD, guideList: List[String]): RDD[CutDS] = {
    val guides = guideList.flatten
    val regions: RDD[(String, ReferenceRegion)] = contigFragmentRDD.findRegions(guideList).flatMapValues(v=>v)
    regions.flatMap{ case (sequence, region) =>
      val start = region.start
      val pams: Seq[(Long, String)] = guideSearch(sequence, false)
      cutsGuided(pams).map{case (guide, (f, b)) =>
        CutDS( guide,
          ReferencePosition(region.referenceName, start + f) ,
          ReferencePosition(region.referenceName, start + b)
        )
      }
    }
  }

  /**
    * All possible cuts inside (note: by now only forward strand is used)
    * @param contigFragmentRDD
    * @return
    */
  def cutome(contigFragmentRDD: NucleotideContigFragmentRDD): RDD[CutDS] = {
    contigFragmentRDD.rdd.flatMap{ fragment=>
      val start = fragment.getFragmentStartPosition
      val sequence = fragment.getFragmentSequence
      val pams: Seq[(Long, String)] = guideSearch(sequence, false)
      cutsGuided(pams).map{case (guide, (f, b)) =>
        CutDS( guide,
          ReferencePosition(fragment.getContig.getContigName, start + f) ,
          ReferencePosition(fragment.getContig.getContigName, start + b)
        )
      }
    }
  }



  /**
    * Warning: works with the forward strand only right now
    * All possible guides for the contigFragment
    * @param contigFragmentRDD genome fragments
    * @param addBefore if we want to get some elements before the start of the guide/pam
    * @param addAfter
    * @param flankAdjacent if we want to flank adjucent fragments (for guides that are inbetween fragments)
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