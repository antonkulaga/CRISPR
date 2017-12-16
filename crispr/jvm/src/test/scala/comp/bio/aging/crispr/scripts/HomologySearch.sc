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


  val base = "hdfs://namenode/pipelines"
  val diamond = base + "/diamond"
  val blastp = diamond + "/blastp"

  def readTSV(path: String) = spark.read.option("sep", "\t").csv(path)

  val unirefPath = blastp + "/graywhale_in_uniref90_blastp.m8"
  val sprotPath = blastp + "/graywhale_in_sprot_blastp.m8"
  val ratPath = blastp + "/graywhale_in_naked_moled_rat_blastp.m8"
  val bowheadPath = blastp + "/graywhale_in_bowhead_blastp.m8"
  val minkyPath = blastp + "/graywhale_in_minkywhale_blastp.m8"
  val humanPath = blastp + "/graywhale_in_human_blastp.m8"
  val mousePath = blastp + "/graywhale_in_mouse_blastp.m8"
  val cowPath = blastp + "/graywhale_in_cow_blastp.m8"
  val wormPath = blastp + "/graywhale_in_worm_blastp.m8"


  val uniref90 = readTSV(unirefPath)
  val sprot = readTSV(sprotPath)
  val rat = readTSV(ratPath)
  val bowhead = readTSV(bowheadPath)
  val minky = readTSV(minkyPath)
  val human = readTSV(humanPath)
  val mouse = readTSV(mousePath)
  val cow = readTSV(cowPath)
  val worm = readTSV(wormPath)



  pprint.pprintln(uniref90.first().toSeq)

  sparkContext.GT

  case class Diamond(
                      id: String,
                      sequence: String,
                    _3,
                      _5,
                    _6,
                    _7,

                    )

  //1. 	 qseqid 	 query (e.g., gene) sequence id
  //2. 	 sseqid 	 subject (e.g., reference genome) sequence id
  //3. 	 pident 	 percentage of identical matches
  //  4. 	 length 	 alignment length
  //  5. 	 mismatch 	 number of mismatches
  //6. 	 gapopen 	 number of gap openings
  //  7. 	 qstart 	 start of alignment in query
  //8. 	 qend 	 end of alignment in query
  //9. 	 sstart 	 start of alignment in subject
  //10. 	 send 	 end of alignment in subject
  //11. 	 evalue 	 expect value
  //  12. 	 bitscore 	 bit score



}
