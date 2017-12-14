package comp.bio.aging.crispr

import com.holdenkarau.spark.testing.SharedSparkContext
import comp.bio.aging.playground.extensions.FeatureType
import org.apache.spark.SparkConf
import org.bdgenomics.adam.models.SequenceRecord
import org.bdgenomics.formats.avro.{Contig, Feature, NucleotideContigFragment, Strand}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

/**
  * Created by antonkulaga on 3/24/17.
  */
class SparkTestBase extends WordSpec with Matchers with BeforeAndAfterAll with SharedSparkContext{

  def sparkContext = sc


  override def conf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator").
      set("spark.kryoserializer.buffer", "16m").
      set("spark.kryoserializer.buffer.max.mb", "1024").
      set("spark.kryo.referenceTracking", "true")
  }


  lazy val test = "test"

  protected def makeFragment(str: String, start: Long) = {

    NucleotideContigFragment.newBuilder()
      .setContigName(test)
      .setStart(start)
      .setLength(str.length: Long)
      .setSequence(str)
      .setEnd(start + str.length)
      .build()
  }

  def makeFeature(sequence: String, start: Long,
                  contigName: String,
                  featureType: FeatureType,
                  geneId: String = "",
                  transcriptId: String = "",
                  exondId: String = ""
                 ): Feature = {
    Feature.newBuilder()
      .setStart(start)
      .setEnd(start + sequence.length)
      .setGeneId(geneId)
      .setTranscriptId(transcriptId)
      .setExonId(exondId)
      .setFeatureType(featureType.entryName)
      .setContigName(contigName)
      .setStrand(Strand.FORWARD)
      .clearAttributes()
      .build()
  }



  def dnas2fragments(dnas: Seq[String]): List[NucleotideContigFragment] = {
    val (_, frags) = dnas.foldLeft((0L, List.empty[NucleotideContigFragment]))
    {
      case ((start, acc), str) => (start + str.length, makeFragment(str, start)::acc)
    }
    frags.reverse
  }
}
