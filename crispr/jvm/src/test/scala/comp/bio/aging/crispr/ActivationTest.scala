package comp.bio.aging.crispr


import com.holdenkarau.spark.testing.SharedSparkContext
import comp.bio.aging.playground.extensions.FeatureType
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.scalatest.{Matchers, WordSpec}
import java.net.URL

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Feature, NucleotideContigFragment, Strand}
import org.scalatest.{Matchers, WordSpec}
import comp.bio.aging.playground._
import comp.bio.aging.playground.extensions._
import org.apache.spark.rdd.RDD

/**
  * Created by antonkulaga on 3/23/17.
  */
class ActivationTest extends SparkTestBase {

  val dnas: Seq[String] = Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT",
    "CAGCTGATCTCCAGATATGACCATGGGTTT",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
  )

  val dnas2 = Vector(
    "ACAGCTGATCTCCAGATATGACCATGGGTT" + "intron",
    "CAGCTGATCTCCAGATATGACCATGGGTTT" + "intron",
    "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
  )

  val merged = dnas.reduce(_ + _)

  val record = SequenceRecord("test", merged.length)


  /*
  val rightResults: Set[(Long, String)] = Set(
    (4L, "CTGATCTCCAGATATGACCATGG"),
    (5L, "TGATCTCCAGATATGACCATGGG"),
    (33L, "CTGATCTCCAGATATGACCATGG"),
    (34L, "TGATCTCCAGATATGACCATGGG"),
    (64L, "AAGTTTGAGCCACAAACCCATGG")
  )
  */

  "CRISPR activation" should {

    "extract transcripts before start site" in {


      val dic = new SequenceDictionary(Vector(record))
      val fragments = NucleotideContigFragmentRDD(sc.parallelize(dnas2fragments(dnas2)), dic)
      val exon1 = makeFeature(dnas.head, 0L, test, FeatureType.Exon, "gene", "transcript", "exon1")
      val intron1 = makeFeature("intron", 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon2 = makeFeature(dnas(1),30L  + 6L,test, FeatureType.Exon, "gene", "transcript", "exon2")
      val intron2 = makeFeature("intron", 30L  + 6L + 30L,test, FeatureType.UTR, "gene", "transcript")
      val exon3 = makeFeature(dnas(2), 30L  + 6L + 30L + 6L,test, FeatureType.Exon, "gene", "transcript", "exon3")
      val tfs = Seq(exon1, intron1, exon2, intron2, exon3)
      val features = FeatureRDD(sc.parallelize(tfs))

      val fs = features.toDF()
      val featureType: FeatureType = FeatureType.Exon
      val tp = featureType.entryName
      val grouping = featureType match {
        case FeatureType.Gene => "geneId"
        case FeatureType.Exon => "exonId"
        case _ => "transcriptId"
      }
      val frags = fragments.toDF()
        .withColumnRenamed("start", "fragment_start")
        .withColumnRenamed("end", "fragment_end")


      val joined = frags.join(fs,
        fs("start") <= frags("fragment_end")
          &&  fs("end") >= frags("fragment_start")
          && fs("contigName") === frags("contigName")
          && fs("featureType") === tp
      )

      val es = features.rdd.map(f=>f.getExonId).collect().toSet
      println("ids = ")
      pprint.pprintln(es)
      val exons = fragments.extractFeatures(features, FeatureType.Exon, es)(f=>f.getExonId).mapValues(_._2).collect.toSet
      exons shouldEqual Set(
        "exon1" -> "ACAGCTGATCTCCAGATATGACCATGGGTT",
        "exon2" -> "CAGCTGATCTCCAGATATGACCATGGGTTT",
        "exon3" -> "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
      )


      val trs = features.rdd.map(f=>f.getTranscriptId).collect().toSet
      val transcripts: Map[String, String] = fragments.extractTranscripts(features, trs).collectAsMap().toMap
      transcripts shouldEqual Map( "transcript" ->  "ACAGCTGATCTCCAGATATGACCATGGGTTCAGCTGATCTCCAGATATGACCATGGGTTTCCAGAAGTTTGAGCCACAAACCCATGGTCA")

    }
  }
}
