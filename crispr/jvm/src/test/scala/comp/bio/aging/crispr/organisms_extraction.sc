import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

val sparkContext: SparkContext

object Cells {
  import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
  import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
  import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
  import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
  import org.bdgenomics.adam.rdd.ADAMContext._
  import comp.bio.aging.playground.extensions._
  import comp.bio.aging.playground.extensions.stringSeqExtensions._
  import org.apache.spark.SparkContext
  import org.apache.spark.sql._

  /* ... new cell ... */

  val root = "hdfs://"//"hdfs://namenode:8020"
  val data = s"${root}/data"
  val genes = s"${data}/genage_LAGs_list.xlsx"

  /* ... new cell ... */

  case class Assembly(species: String, path: String)
  {
    val genomeAdam = s"${path}/genome.adam" //genome release
  val featuresAdam = s"${path}/features.adam" //features
  }

  //val human = Assembly("human", s"${root}/genomes/HUMAN/26")
  val mouse = Assembly("Mus musculus", s"${root}/genomes/MOUSE/M14")
  val worm = Assembly("Caenorhabditis elegans", s"${root}/genomes/WORM/WS260")
  val fly = Assembly("Drosophila melanogaster", s"${root}/genomes/FLY/r6.16")
  val yeast = Assembly("Saccharomyces cerevisiae", "") //toDO: add

  sparkContext.loadGtf(s"${current.path}/gencode.vM14.chr_patch_hapl_scaff.annotation.gtf").saveAsParquet(current.featuresAdam)

  /* ... new cell ... */

  val current = mouse
  val genome = sparkContext.loadParquetContigFragments(current.genomeAdam)
  val features = sparkContext.loadParquetFeatures(current.featuresAdam)

  /* ... new cell ... */

  val genageHuman = "genage_human"
  val genageModels = "genage_models"
  val currentSheet = genageModels

  val datasets = s"${root}/datasets"
  val excelFile = s"${datasets}/genage_LAGs_list.xlsx"

  val session = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val lags: DataFrame = session.read
    .format("com.crealytics.spark.excel")
    .option("location", excelFile)
    .option("sheetName", currentSheet)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "true")
    .load().withColumnRenamed("entrez gene id", "entrez")

  /* ... new cell ... */

  val metadata = s"${current.path}/metadata"
  val entrezGenes = s"$metadata/gencode.vM14.metadata.EntrezGene"
  val entrezNames = s"$metadata/gencode.vM14.metadata.MGI"

  /* ... new cell ... */

  import session.implicits._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

  val idsTable = session.read.option("sep", "\t").csv(entrezGenes)
    .withColumnRenamed("_c0", "feature_id")
    .withColumnRenamed("_c1", "id")
  val names = session.read.option("sep", "\t").csv(entrezNames)
    .withColumnRenamed("_c0", "transcript_id")
    .withColumnRenamed("_c1", "transcript_name")

  val ids = idsTable.join(names, names("transcript_id") === idsTable("feature_id"))
    .withColumn("id", idsTable("id").cast(IntegerType))

  /* ... new cell ... */

  import org.apache.spark.storage.StorageLevel
  val selection = lags.withColumn("entrez", lags("entrez").cast(IntegerType)) // excel messed the field type, fixing it
    .where($"organism" === current.species)
    //.select($"entrez", $"GenAge ID", $"avg lifespan change (max obsv)", $"references") // we do not need all fields
    //GenAge ID	symbol	name	organism	entrez gene id	avg lifespan change (max obsv)	lifespan effect	longevity influence

  val lagsWithId = selection
    .join(ids, $"entrez" === $"id")
    .orderBy($"entrez")


  //GenAge ID	symbol	name	organism	entrez gene id	avg lifespan change (max obsv)	lifespan effect	longevity influence
  val transcripts = lagsWithId
    .join(features.toDF.where($"featureType" === "transcript"), $"feature_id" === $"transcriptId")
    .select($"GenAge ID", $"transcriptId", $"geneId", $"name", $"entrez",
      $"avg lifespan change (max obsv)",	$"lifespan effect",	$"longevity influence",
      $"source", $"contigName", $"start", $"end",
      $"strand", $"attributes", $"exonId", $"featureType", $"transcript_name")
    .orderBy($"GenAge ID")
    .persist(StorageLevel.MEMORY_AND_DISK)

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
      Math.round(r.getAs[Double]("GenAge ID")).toString +  "|" +
      r.getAs[String]("transcriptId")  + "|" + r.getAs[String]("transcript_name") + "|" +
      r.getAs[String]("geneId") + "|" + r.getAs[String]("entrez") + "|" +
      r.getAs[String]("organism")  + "|" +r.getAs[String]("avg lifespan change (max obsv)")  + "|" +
      r.getAs[String]("lifespan effect")  + "|" + r.getAs[String]("longevity influence")  + "|" +
      r.getAs[String]("contigName") + "|" +
      r.getAs[String]("start") + "|" + r.getAs[String]("end") + "|" + r.getAs[String]("strand")
    r.getAs[String]("sequence").sliding(80, 80).mkString(header+"\n", "\n", "\n")
  }
  //$"avg lifespan change (max obsv)",	$"lifespan effect",	$"longevity influence",


  /* ... new cell ... */

  (rows.count, transcripts.count, tf.count)

  /* ... new cell ... */

  rows.coalesce(1).saveAsTextFile(s"${datasets}/mitya/${current.species}/transcripts.fasta")

  /* ... new cell ... */
}
