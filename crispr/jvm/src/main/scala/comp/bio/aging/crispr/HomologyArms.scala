package comp.bio.aging.crispr

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, ReferenceRegion}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import comp.bio.aging.playground.extensions._
import scala.collection.immutable.{List, Nil}



trait HomologyArms {

  def arms(fragmentRDD: NucleotideContigFragmentRDD,
           cuts: RDD[CutDS],
           left: Long, right: Long, avoidSites: Set[String] = Set.empty, allowOverlap: Boolean = true): RDD[KnockIn] = {

    val positiveCuts: RDD[(ReferenceRegion, CutDS)] = cuts.filter(_.positive(left)).map{
      case (cut) => cut.armsRegion(left, right) -> cut
    }.persist(StorageLevels.MEMORY_AND_DISK)

    val extracted = fragmentRDD.extractRegions(positiveCuts.keys.collect().toList)

    val joined: RDD[(ReferenceRegion, (CutDS, String))] = positiveCuts.join(extracted) //region,guide, value
    joined.map{
      case (region, (cut, regionSeq)) => cut.knockin(regionSeq, region, left, right, allowOverlap)
    }
  }
}
