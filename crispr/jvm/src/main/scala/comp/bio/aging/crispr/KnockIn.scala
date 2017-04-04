package comp.bio.aging.crispr

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, ReferenceRegion}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import comp.bio.aging.playground.extensions._
import scala.collection.immutable.{List, Nil}

object CutDS{
  //for Blunt edges
  def apply(guide: String, top: ReferencePosition): CutDS = new CutDS(guide, top, top)
}

/**
  * Double stranded cut
  * @param guide guideRNA
  * @param top top strand cut position
  * @param bottom bottom strand cut position
  */
case class CutDS(guide: String, top: ReferencePosition, bottom: ReferencePosition)
{
  lazy val leftishCut: ReferencePosition =  if(top.pos <= bottom.pos) top else bottom
  lazy val rightishCut: ReferencePosition = if(top.pos >= bottom.pos) top else bottom

  def leftArm(length: Long, canOverlap: Boolean = true): ReferenceRegion = {
    ReferenceRegion(leftishCut.referenceName,
      leftishCut.pos - length,
      (if(canOverlap) rightishCut else leftishCut).pos,
      strand = leftishCut.strand)
  }

  def rightArm(length: Long, canOverlap: Boolean = true): ReferenceRegion = {
    ReferenceRegion(rightishCut.referenceName,
      (if(canOverlap) leftishCut else rightishCut).pos,
      rightishCut.pos + length,
      strand = rightishCut.strand)
  }

  def arms(length: Long): List[ReferenceRegion] = arms(length, length)

  def arms(leftLength: Long, rightLength: Long) = List(leftArm(leftLength), rightArm(rightLength))

  def armsRegion(leftLength: Long, rightLength: Long): ReferenceRegion = {
    ReferenceRegion(top.referenceName,
      leftishCut.pos - leftLength,
      rightishCut.pos + rightLength,
      strand = top.strand)
  }

  def knockin(regionSeq: String, region: ReferenceRegion, leftLength: Long, rightLength: Long, overlap: Boolean): KnockIn = {
    val left = leftArm(leftLength, overlap)
    val right = rightArm(rightLength, overlap)
    val leftSeq = regionSeq.take(left.length().toInt)
    val rightSeq = regionSeq.takeRight(right.length().toInt)
    KnockIn(guide, leftSeq, left,  rightSeq, right)
  }

  def positive(length: Long): Boolean = (leftishCut.pos - length) >= 0

}

case class KnockIn(guide: String, leftArm: String, leftArmRegion: ReferenceRegion, rightArm: String, rightArmRegion: ReferenceRegion)