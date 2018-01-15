import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


val spark = SparkSession
  .builder()
  .appName("Homology search")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
spark.sparkContext.setLogLevel("WARN")

import org.apache.spark.SparkContext

val sparkContext: SparkContext


object Cells {
  import org.apache.spark.sql.SparkSession
  import org.bdgenomics.adam.rdd.ADAMContext._

  val spark = SparkSession
    .builder()
    .appName("Homology search")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  spark.sparkContext.setLogLevel("WARN")

  /* ... new cell ... */

  val base = "hdfs://namenode/pipelines"
  val diamond = base + "/diamond"
  val blastp = diamond + "/blastp"
  val graywhale = base + "/GRAY_WHALE"
  val trinity = graywhale + "/Trin_Mitya.Trinity.fasta"
  val proteins = graywhale + "/Trin_Mitya.Trinity.fasta.transdecoder.pep"
  val gff = graywhale + "/Trin_Mitya.Trinity.fasta.transdecoder.gff3"

  def readTSV(path: String) = spark.read.option("sep", "\t").csv(path)

  /* ... new cell ... */

  val proteinsAdam = graywhale + "/proteins.adam"
  val transcriptsAdam = graywhale + "/transcripts.adam"

  def convert() = {
    val transcripts = sparkContext.loadFasta(trinity)
    val query = sparkContext.loadFasta(proteins)
    query.saveAsParquet(proteinsAdam)
  }

  /* ... new cell ... */

  val transcripts = sparkContext.loadParquetContigFragments(transcriptsAdam)
  val query = sparkContext.loadParquetContigFragments(proteinsAdam)

  /* ... new cell ... */

  print(transcripts.sequences.size)
  print("===")
  print(transcripts.rdd.count)
  println("----------------")
  print(query.sequences.size)
  print("===")
  print(query.rdd.count)

  /* ... new cell ... */

  import scala.util.Try
  trait ProteinSearch{
    def id: String
    def e: String
  }

  case class BlastResult(id: String, score: Double, e: String) extends ProteinSearch
  case class PfamResult(domain: String, id: String, e: String) extends ProteinSearch

  case class ProteinPrediction(transcript: String,
                               id: String,
                               sequence: String,
                               orf_type: String,
                               score: Double,
                               start: Long,
                               end: Long,
                               len: Int,
                               strand: Char,
                               diamondHits: List[BlastResult],
                               pfamHits: List[PfamResult]
                              )

  def extractPrediction(description: String, sequence: String): ProteinPrediction = {
    val gene::transcript::rest::Nil = description.split("::").toList
    val id::orf::tp::len_string::params::tr::Nil = rest.split(' ').filter(_!="").toList
    val orf_type: String = tp.substring(tp.indexOf(':') + 1)
    val _::etc::Nil = tr.split(':').toList
    val str::score_string::other = params.split(',').toList
    val strand: Char = if(str.contains("-")) '-' else '+'
    val len = Integer.parseInt(
      len_string.substring(len_string.indexOf(':') + 1)
    )
    val (diamondHits, pfamHits) = other.foldLeft((List.empty[BlastResult], List.empty[PfamResult])){
      case ((b, p), el) =>
        val id::value::e::Nil = el.split('|').toList
        Try(value.toDouble).map(
          v=>
            (BlastResult(id, v, e)::b, p)
        ).getOrElse(
          (b, PfamResult(id, value, e)::p)
        )
    }
    val span = etc.substring(0, etc.indexOf('('))
    val start::end::Nil = span.split('-').map(_.toLong).toList
    val score = score_string.substring(score_string.indexOf('=') + 1).toDouble
    ProteinPrediction(
      transcript, id, sequence, orf_type, score, start, end, len, strand, diamondHits, pfamHits
    )
  }

  /* ... new cell ... */

  val predictions: RDD[(String, ProteinPrediction)] = query.rdd
    .map(q=>extractPrediction(q.getDescription, q.getSequence))
    .filter(p=>p.diamondHits.nonEmpty || p.pfamHits.nonEmpty)
    .map(p=>(p.transcript, p))
    //.persist(StorageLevel.MEMORY_AND_DISK_SER)

  /* ... new cell ... */


  val mapping = readTSV("/pipelines/idmapping_selected.tab")
  mapping.head()


  //val names = transcripts.rdd.map(i=>(i.getContigName, i.getDescription))
  //names.keys.intersection(byTranscript.keys)

  //val joins = byTranscript.join(names)

  //val contigs = transcripts.rdd.map(t=> (t.getContigName , t))
}
