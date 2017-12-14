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

  def guidomeByGuideList(guidomeFragments: NucleotideContigFragmentRDD,
                         guideList: List[String],
                       pamsInGuidome: Boolean = true): RDD[(String, List[NucleotideContigFragment])] = {
   val guides = guideList.distinct
   if(pamsInGuidome) {
     if(guideEnd < 0) guidomeFragments.rdd.flatMap{ fr =>
       val seq = fr.getSequence.substring(0, fr.getLength.toInt - pam.length)
       guides.collect{ case g if g == seq => g -> List(fr)}
     } else {
       guidomeFragments.rdd.flatMap{ fr=>
         val seq = fr.getSequence.substring(pam.length)
         guides.collect{ case g if g == seq => g -> List(fr)}
       }
     }
   } else guidomeFragments.rdd.flatMap{ fr=>
       guides.collect{ case g if g == fr.getSequence => g -> List(fr)}
     }
  }.reduceByKey(_ ++ _)

  def cutomeFromGuideFragments(guideFragments: RDD[(String, List[NucleotideContigFragment])]): RDD[(String, List[CutDS])] = {
      guideFragments.mapValues{ frgs=> frgs.map{ fragment =>
        val guide = if(guideEnd < 0 )
          fragment.getSequence.substring(0, fragment.getLength.toInt - pam.length)
        else
          fragment.getSequence.substring(pam.length)
          val pamEdge: Long = if(guideEnd >= 0 ) fragment.getStart + pam.length else fragment.getEnd - pam.length
          CutDS( guide,
            ReferencePosition(fragment.getContigName, pamEdge + forwardCut) ,
            ReferencePosition(fragment.getContigName, pamEdge + reverseCut)
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
      val start = fragment.getStart
      val sequence = fragment.getSequence
      val guides: Seq[(Long, String)] = guideSearchIn(sequence, false)
      cutsGuided(guides, false).map{case (guide, (f, b)) =>
        CutDS( guide,
          ReferencePosition(fragment.getContigName, start + f) ,
          ReferencePosition(fragment.getContigName, start + b)
        )
      }
    }
  }

  def filterByGC(contigFragmentRDD:NucleotideContigFragmentRDD, minPercent: Int = 20, maxPercent: Int = 75): NucleotideContigFragmentRDD = {
    contigFragmentRDD.transform{
      rdd=> rdd.filter{
        frag =>
          val seq = frag.getSequence.toUpperCase
          val percent = seq.count{
            case 'G' | 'C' => true
            case _ => false
          } * 100 / seq.length
          minPercent < percent && percent < maxPercent
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

      guideSearchIn(fragment.getSequence, includePam, addBefore, addAfter).map{
        case (index, seq)=>
          NucleotideContigFragment
            .newBuilder(fragment)
            .setStart(fragment.getStart + index)
            .setEnd(fragment.getStart + (index + seq.length))
            .setIndex(null)
            .setSequence(seq)
            .setLength(seq.length: Long)
            .build()
      }
    }

    fragments.transform{
      rdd=>
        rdd.flatMap{ fragment=> extractForwardGuideFragments(fragment, includePam, addBefore, addAfter)}
    }
  }

}