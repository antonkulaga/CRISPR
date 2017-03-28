package comp.bio.aging.crispr

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, ReferenceRegion}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import comp.bio.aging.playground.extensions._
import scala.collection.immutable.{List, Nil}


case class KnockIn(guide: String, leftArm: String, leftArmRegion: ReferenceRegion, rightArm: String, rightArmRegion: ReferenceRegion)


trait HomologyArms {

  def arms(fragmentRDD: NucleotideContigFragmentRDD,
           cuts: RDD[(String, (ReferencePosition, ReferencePosition))],
           left: Long, right: Long, avoidSites: Set[String] = Set.empty): RDD[(String, KnockIn)] = {

    def position2region(pos: ReferencePosition, shift: Long): ReferenceRegion = {
      ReferenceRegion(pos.referenceName,
        pos.pos + (if(shift < 0) shift else 0),
        pos.pos + (if(shift >= 0) shift else 0),
        strand = pos.strand)
    }

    val armRegions: RDD[(ReferenceRegion, String)] = cuts
      .filter{ case (str, _) => !avoidSites.exists(a=>str.contains(a))}
      .flatMap{ case (str, (forward, backward)) =>
        val leftCut: ReferencePosition = if(forward.pos >= backward.pos) forward else backward
        val rightCut: ReferencePosition = if(forward.pos <= backward.pos) forward else backward
        val leftArm = position2region(leftCut, - left)
        val rightArm = position2region(rightCut, right)
        (leftArm, rightArm)
        List(leftArm->str, rightArm->str)
      }.persist(StorageLevels.MEMORY_AND_DISK)

    val regions = armRegions.keys.collect().toList
    val extracted: RDD[(ReferenceRegion, String)] = fragmentRDD.extractRegions(regions)

    val joined: RDD[(ReferenceRegion, (String, String))] = armRegions.join(extracted) //region,guide, value
    val groupedJoin = joined.groupBy{ case (_, (guide, _)) => guide }

    groupedJoin.flatMapValues{ iter =>
      iter.toList match {
        case (l, (guide, vLeft))::(r, (g, vRight))::Nil if l.start <= r.start  => List(KnockIn(guide, vLeft,l, vRight, r))
        case (r, (g, vRight))::(l, (guide, vLeft))::Nil  => List(KnockIn(guide, vLeft,l, vRight, r))
        case rs =>
          println(s"REGIONS of length ${rs.length} do not make a homology pair!")
          pprint.pprintln(rs)
          Nil
      }
    }
  }
}
