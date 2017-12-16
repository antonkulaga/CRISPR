import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.Feature

import scala.collection.immutable.{Seq, Set}

val sparkContext: SparkContext


object Cells {
  import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
  import comp.bio.aging.playground.extensions._
  import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
  import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
  import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
  import org.bdgenomics.adam.rdd.ADAMContext._
  import comp.bio.aging.playground.extensions.stringSeqExtensions._
  import org.apache.spark.SparkContext
  import org.apache.spark.sql._

  /* ... new cell ... */

  val root = "hdfs://"//"hdfs://namenode:8020"
  val data = s"${root}/data"

  /* ... new cell ... */

  case class Assembly(species: String, path: String)
  {
    val genomeAdam = s"${path}/genome.adam" //genome release
    val featuresAdam = s"${path}/features.adam" //features
  }

  val human = Assembly("human", s"${root}/genomes/HUMAN/26")
  val mouse = Assembly("mouse", s"${root}/genomes/MOUSE/M14")

  /* ... new cell ... */

  val current = human
  val genome = sparkContext.loadParquetContigFragments(current.genomeAdam)
  val features = sparkContext.loadParquetFeatures(current.featuresAdam)

  /* ... new cell ... */

  val session = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  /* ... new cell ... */

  import session.implicits._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

  val orderPath = "/tmp/order.csv"

  val order = session.read
    .option("header", true)
    .option("sep", "\t")
    .csv(orderPath)
  order.show(100)

  /* ... new cell ... */

  val left_left  = "ACGCGT"
  val left_right = "AGATCTAATGA"
  val right_left =  "TCGCGACTCGAG"
  val right_right = "GGCGCGCCaatctttacaGCGGCCGC"

  /* ... new cell ... */

  import session.implicits._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._
  import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
  import comp.bio.aging.playground.extensions._
  import org.bdgenomics.adam.models.ReferenceRegion

  val id = "ID"
  val arm = "Homology arms sequence"
  val guide = "Guide RNA"

  case class Arm(id: String, sequence: String, guideRNA: String)  {

    val left_left = "GCACGCGT"
    //val left_left  = "ACGCGT"
    val left_right = "AGATCTAATGA"
    val right_left =  "TCGCGACTCGAG"
    val right_right = "GGCGCGCCaatctttacaGCGGCCGC".toUpperCase
    //val right_right = "GGCGCGCCAATCTTTACAGCGGCCGC"


    lazy val seq = sequence.toUpperCase


    lazy val leftArm = {
      seq.substring(
        seq.indexOf(left_left)+left_left.length,
        seq.indexOf(left_right)
      )
    }

    lazy val rightArm = {
      seq.substring(
        seq.indexOf(right_left)+right_left.length,
        seq.indexOf(right_right)
      )
    }

    lazy val whole = leftArm + rightArm

    def valid = whole contains guideRNA

    if(!valid) println(s"$id should contain guideRNA")

    def takeFrom(fragments: NucleotideContigFragmentRDD, str: String, shift: Long): ReferenceRegion = {
      val region = fragments.search(str).first()
      shift match {
        case 0 => region

        case a if a < 0 =>
          region.copy(start = region.start + shift, end = region.start)

        case a =>
          region.copy(start = region.end, end = region.end + shift)
      }
    }

    def flanks(fragments: NucleotideContigFragmentRDD, distance: Long, part: Int = 100): ((ReferenceRegion, String), (ReferenceRegion, String)) = {
      val left = takeFrom(fragments, leftArm.take(part), -distance)
      val right = takeFrom(fragments, rightArm.takeRight(part), distance)
      (
        left -> fragments.extract(left),
        right -> fragments.extract(right)
      )
    }

  }

  /* ... new cell ... */

  import session._
  import session.implicits._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

  val armsData = order.rdd.filter(r=> !r.getAs[String](id).contains("insulator")).map {
    row =>
      val hid =  row.getAs[String](id)
      val seq = row.getAs[String](arm)
      val sg = row.getAs[String](guide)
      ( hid, seq, sg )//( hid, seq, sg )
  }
  //arms.collect

  /* ... new cell ... */

  val arms = armsData.collect.map{ case (hig, seq, sg) => val arm = Arm(hig, seq, sg); (arm, arm.flanks(genome, 300L))}

  /* ... new cell ... */
  val sparkContext: SparkContext

  val rows = arms.map{ case (arm, ((leftRegion, leftSeq), (rightRegion, rightSeq))) =>
    (arm.id, arm.guideRNA, leftRegion.start, leftRegion.end, leftSeq, arm.leftArm, arm.rightArm, rightRegion.start, rightRegion.end, rightSeq)
  }


  sparkContext.parallelize(rows, 1).toDF(
    "id", "guide",
    "start_left_300", "end_left_300", "left_sequence",
    "left_Arm",
    "right_Arm",
    "start_right_300", "end_right_300", "right_sequence",
  ).coalesce(1).write
    .option("header", true)
    .option("delimiter", "\t")
    .csv("/tmp/arms.tsv")

  /* ... new cell ... */

}
