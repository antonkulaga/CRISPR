
object Cells {
  import org.bdgenomics.adam.rdd.ADAMContext._
  import comp.bio.aging.playground.extensions._
  import org.apache.spark.sql._
  import org.apache.spark.sql.SparkSession
  import frameless.functions.aggregate
  import frameless.TypedDataset
  import frameless.syntax._

  implicit val session = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  /* ... new cell ... */

  val base = "hdfs://namenode/pipelines"
  val ml = s"${base}/ml"
  val genAgePath = s"${ml}/genage_LAGs_list.xlsx"
  val vladFilePath = s"${ml}/ML validation (11.12.2017).xlsx"


  /* ... new cell ... */

  def openSpreadsheet(location: String, sheet: String, emptyAsNulls: Boolean = false, colorColumns: Boolean = false)
                     (implicit sessions: SparkSession): DataFrame = {
    sessions.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", sheet) // Required
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", emptyAsNulls.toString) // Optional, default: true
      .option("inferSchema", "true") // Optional, default: false
      .option("addColorColumns", colorColumns.toString) // Optional, default: false
      .load(location)
  }

  /* ... new cell ... */

  val genAgeHuman = openSpreadsheet(genAgePath, "genage_human")
    .withColumnRenamed("entrez gene id", "entrez").cache
  genAgeHuman

  /* ... new cell ... */

  val genAgeModel = openSpreadsheet(genAgePath, "genage_models").withColumnRenamed("entrez gene id", "entrez").cache
  genAgeModel

  /* ... new cell ... */

  import frameless.TypedDataset
  import frameless.syntax._
  import session.implicits._

  import org.apache.spark.sql.functions._

  val toDouble = udf[Double, String](s => if(s!=null) s.replace(",","").toDouble else 0.0)

  val vladGenes = Seq("WRN", "LMNA", "FOXO1", "APOE", "Gdf15", "GHR", "Ghrh", "IGF1", "Pou1f1", "age-1", "daf-2", "Tor", "osm-5", "unc-13")
  def openVladSheet(sheet: String) = {
    val df = openSpreadsheet(vladFilePath, sheet)
    df
      //.withColumn("Entrez", toInt(df("Entrez")))
      .withColumn("Score", toDouble(df("Score")))
      //.withColumn("TaxID", toInt(df("TaxID")))
      .withColumnRenamed("Effect on CS (CellAge)", "CellAge")
      //.withColumnRenamed("C6", "GenAge")
      .withColumn("predicted_for", lit(sheet))
      .select("predicted_for", "ORD", "Entrez",   "TaxID", "Score", "Symbol", "CellAge", "Comment")
  }
  val predictions = vladGenes.map(g=>openVladSheet(g)).reduce((a,b)=> a.union(b)).cache
  predictions

  /* ... new cell ... */

  predictions.show(100)

  /* ... new cell ... */

  genAgeModel

  /* ... new cell ... */

  val modeled = predictions
    .join(genAgeModel, genAgeModel("entrez") === predictions("Entrez"))
    .drop(genAgeModel("entrez"))
    .drop(genAgeModel("symbol"))
    .cache
  modeled

  /* ... new cell ... */

  val humaned  = predictions
    .join(genAgeHuman, predictions("Entrez") === genAgeHuman("entrez"))
    .drop(genAgeHuman("entrez"))
    .drop(genAgeHuman("symbol"))
    .cache
  humaned

  /* ... new cell ... */

  val predictedIds =
    modeled.select("Entrez").map(_.getDouble(0)).collect() ++ humaned.select("Entrez").map(_.getDouble(0)).collect()
  val notInGenAge = predictions.filter( r=> !predictedIds.contains(r.getAs[Double]("Entrez")))
  notInGenAge

  /* ... new cell ... */

  modeled.coalesce(1).write.option("sep", "\t").csv(ml + "/predictions_models.tsv")
  humaned.coalesce(1).write.option("sep", "\t").csv(ml + "/predictions_human.tsv")
  notInGenAge.coalesce(1).write.option("sep", "\t").csv(ml + "/predictions_notfound.tsv")

  /* ... new cell ... */

  (predictions.count, modeled.count, humaned.count, notInGenAge.count)

  /* ... new cell ... */
}
                  