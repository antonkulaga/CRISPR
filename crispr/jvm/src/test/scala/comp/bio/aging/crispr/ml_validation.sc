import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

val sparkContext: SparkContext


object Cells {

  import org.bdgenomics.adam.rdd.ADAMContext._
  import comp.bio.aging.playground.extensions._
  import org.apache.spark.sql._

  /* ... new cell ... */

  val root = "hdfs://"//"hdfs://namenode:8020"
  val data = s"${root}/pipelines/ml"
  val genAgePath = s"${data}/genage_LAGs_list.xlsx"
  val vladFilePath = s"${data}/ML validation (11.12.2017).xlsx"

  val session = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val genAgeHuman = "genage_human"
  val genAgeModel = "genage_models"

  val lagsModel: DataFrame = session.read
    .format("com.crealytics.spark.excel")
    .option("location", genAgePath)
    .option("sheetName", genAgeModel)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "true")
    .load().withColumnRenamed("entrez gene id", "entrez")

  val lagsHuman: DataFrame = session.read
    .format("com.crealytics.spark.excel")
    .option("location", genAgePath)
    .option("sheetName", genAgeHuman)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "true")
    .load().withColumnRenamed("entrez gene id", "entrez")

  /* ... new cell ... */

  val metadata = s"${current.path}/metadata"
  val entrezGenes = s"$metadata/gencode.v26.metadata.EntrezGene"

  /* ... new cell ... */

  import session.implicits._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

  case class FeatureId(feature_id: String, id: Int)
  val idsTable = session.read.option("sep", "\t").csv(entrezGenes)
    .withColumnRenamed("_c0", "feature_id")
    .withColumnRenamed("_c1", "id")
  val ids =   idsTable.withColumn("id", idsTable("id").cast(IntegerType)).as[FeatureId]

  /* ... new cell ... */

  import org.apache.spark.storage.StorageLevel
  val selection = lags.withColumn("entrez", lags("entrez").cast(IntegerType)) // excel messed the field type, fixing it
    .select($"entrez", $"GenAge ID", $"why", $"references") // we do not need all fields
  val lagsWithId = selection.join(ids, $"entrez" === $"id")
    .orderBy($"entrez")

  val transcripts = lagsWithId
    .select($"GenAge ID", $"transcriptId", $"geneId", $"entrez", $"why",
      $"source", $"contigName", $"start", $"end",
      $"strand", $"attributes", $"exonId", $"featureType")
    .orderBy($"GenAge ID")

  (selection.count, lagsWithId.count, transcripts.count)

  /* ... new cell ... */

  val stringify = udf((vs: Seq[Any]) => s"""[${vs.mkString(",")}]""") //we have to stringify arrays to write them to tsv
  val stringifyMap = udf((vs: Map[String, String]) => s"""[${vs.mkString(",")}]""") //we have to stringify maps to write them to tsv

  transcripts
    //which fields we want to write to tsv
    .withColumn("attributes", stringifyMap($"attributes"))
    .coalesce(1).write
    .option("header", "true")
    .option("delimiter", "\t")
  //.csv(s"${datasets}/mitya/human/index.tsv")

  /* ... new cell ... */

  val ts = transcripts.select($"transcriptId").as[String].collect().toSet
  val trans: RDD[(String, String)] = genome.extractTranscripts(features, ts).persist(StorageLevel.MEMORY_AND_DISK)
  trans.count

  /* ... new cell ... */

  val tf = trans.toDF("transcriptId", "sequence")
  val joined = tf.join(transcripts, tf("transcriptId") === transcripts("transcriptId"))
  val rows: RDD[String] = joined.rdd.map{ r=>
    val header = ">" +
      r.getAs[Int]("GenAge ID").toString + "|" + r.getAs[String]("why") + "|" +
      r.getAs[String]("transcriptId") + "|" + r.getAs[String]("geneId") + "|" + r.getAs[String]("entrez") + "|" +
      r.getAs[String]("contigName") + "|" + r.getAs[String]("start") + "|" + r.getAs[String]("end") + "|" + r.getAs[String]("strand")
    r.getAs[String]("sequence").sliding(80, 80).mkString(header+"\n", "\n", "\n")
  }

  /* ... new cell ... */

  (rows.count, transcripts.count, tf.count)

  /* ... new cell ... */

  rows.coalesce(1).saveAsTextFile(s"${datasets}/mitya/${current.species}/transcripts.fasta")

  /* ... new cell ... */

}
