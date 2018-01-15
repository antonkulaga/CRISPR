import org.apache.spark.SparkContext

val sparkContext: SparkContext



object Cells {
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.sql.SparkSession
  import frameless.functions.aggregate._
  import frameless.TypedDataset
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  /* ... new cell ... */

  val base = "hdfs://namenode/pipelines"
  val diamond = base + "/diamond"
  val blastp = diamond + "/blastp"
  val graywhale = base + "/GRAY_WHALE"
  //val graywhaleProteins = graywhale + "/NTJE01P.1.fasta"
  val grayWhaleTranscripts = graywhale + "// pipelines/ GRAY_WHALE/ Trin_Mitya.Trinity.fasta"
  val graywhaleProteins = graywhale + "/proteins.adam"

  def readTSV(path: String) = spark.read.option("sep", "\t").csv(path)

  /* ... new cell ... */

  val unirefPath = blastp + "/graywhale_in_uniref90_blastp.m8"
  val sprotPath = blastp + "/graywhale_in_sprot_blastp.m8"
  val ratPath = blastp + "/graywhale_in_naked_moled_rat_blastp.m8"
  val bowheadPath = blastp + "/graywhale_in_bowhead_blastp.m8"
  val minkyPath = blastp + "/graywhale_in_minkywhale_blastp.m8"
  val humanPath = blastp + "/graywhale_in_human_blastp.m8"
  val mousePath = blastp + "/graywhale_in_mouse_blastp.m8"
  val cowPath = blastp + "/graywhale_in_cow_blastp.m8"
  val wormPath = blastp + "/graywhale_in_worm_blastp.m8"

  /* ... new cell ... */

  val uniref = readTSV(unirefPath)
  val sprot = readTSV(sprotPath)
  val rat = readTSV(ratPath)
  val bowhead = readTSV(bowheadPath)
  val minky = readTSV(minkyPath)
  val human = readTSV(humanPath)
  val mouse = readTSV(mousePath)
  val cow = readTSV(cowPath)
  val worm = readTSV(wormPath)


  /* ... new cell ... */

  import org.bdgenomics.adam.rdd.ADAMContext._
  import comp.bio.aging.playground.extensions._
  import org.apache.spark.sql._
  val query = sparkContext.loadFasta()

  /* ... new cell ... */

  query.saveAsParquet(graywhaleAdam)

  /* ... new cell ... */

  val seqs = query.sequences.records.map(r=>r)
  (query.rdd.count, seqs.count)

  /* ... new cell ... */

  seqs.first

  /* ... new cell ... */

  case class DiamondResult(sseqid: String, //Subject Seq - id
                           qseq: String, // Aligned part of query sequence
                           score: String, //Raw score
                           pident: String, //Percentage of identical matches
                           stitle: String, //Subject Title
                           qcovhsp: String, //Query Coverage Per HSP
                           qtitle: String // Query title
                          )

  /* ... new cell ... */

  uniref.schema

  /* ... new cell ... */

  //uniref.show(10)
  //sprot.show(10)
  uniref90.schema
  pprint.pprintln(uniref.first().toSeq)
  pprint.pprintln(sprot.first().toSeq)
  pprint.pprintln(rat.first().toSeq)
  pprint.pprintln(bowhead.first().toSeq)
  pprint.pprintln(human.first().toSeq)
  pprint.pprintln(mouse.first().toSeq)
  pprint.pprintln(cow.first().toSeq)
  pprint.pprintln(worm.first().toSeq)

  /* ... new cell ... */

  val g = sprot.groupBy("_c6")
  (sprot.count(), g.count())

  /* ... new cell ... */
}
