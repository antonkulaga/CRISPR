import org.apache.spark.SparkContext
val sparkContext: SparkContext

object Cells {
  import org.apache.spark.rdd.RDD
  import org.bdgenomics.adam.models.ReferenceRegion

  import scala.collection.immutable.Set
  import comp.bio.aging.playground.extensions._
  import org.apache.spark.sql._
  import org.bdgenomics.adam.rdd.ADAMContext._

  /* ... new cell ... */

  val root = "hdfs://"//"hdfs://namenode:8020"
  val assembly = s"${root}/genomes/HUMAN/26" //prefix
  val genomeAdam = s"${assembly}/genome.adam" //genome release
  val featuresAdam = s"${assembly}/features.adam" //features
  val datasets = s"${root}/datasets"
  val metadata = s"${assembly}/metadata"
  val entrezGenes = s"$metadata/gencode.v26.metadata.EntrezGene"
  val entrezNames = s"$metadata/gencode.v26.metadata.HGNC"
  val hgnc = s"$metadata/hgnc.tsv"

  /* ... new cell ... */

  val genome = sparkContext.loadParquetContigFragments(genomeAdam)
  val features = sparkContext.loadParquetFeatures(featuresAdam)
  val session = SparkSession
    .builder()
    .appName("CRISPR")
    .getOrCreate()

  /* ... new cell ... */

  import session.implicits._

  val guideListPath = s"${datasets}/sam-target-sequences.csv"
  val guideList = session.read
    .option("sep", ",")
    .option("header", "true")
    .csv(guideListPath)
    .withColumnRenamed("guide sequence", "sequence")
    .withColumnRenamed("gene ", "refseq")
  guideList.show

  /* ... new cell ... */

  val geneNames = session.read
    .option("sep", "\t")
    .option("header", true)
    .csv(hgnc)
    .withColumnRenamed("_c0", "transcript_id")
    .withColumnRenamed("_c1", "refseq_1")
    .withColumnRenamed("_c2", "refseq_2")
  geneNames.show

  /* ... new cell ... */

  val geneInfoPath = s"${metadata}/Homo_sapiens.gene_info"
  val geneInfo = session.read
    .option("sep", "\t")
    .option("header", true)
    .csv(geneInfoPath)
    .select("GeneID", "dbXrefs")//, "Symbol_from_nomenclature_authority")
    .map { case r =>
      val entrez = r.getAs[String]("GeneID")
      val ids = r.getAs[String]("dbXrefs")
      val ens = "Ensembl:"
      ids.indexOf(ens) match {
        case -1 => (entrez, "")
        case i =>
          val from = i + ens.length
          ids.indexOf("|", from) match {
            case -1 =>
              (entrez, ids.substring(from))
            case j =>
              (entrez, ids.substring(from, j))
          }
      }
    }.withColumnRenamed("_1", "geneId").withColumnRenamed("_1", "geneId")
  geneInfo.printSchema()
  geneInfo.show
  /* ... new cell ... */

  val idsTable = session.read.option("sep", "\t").csv(entrezGenes)
    .withColumnRenamed("_c0", "feature_id")
    .withColumnRenamed("_c1", "id")
  idsTable.show

  /* ... new cell ... */

  val guideTable = guideList.join(geneNames, $"RefSeq IDs" === $"refseq")
    .select("Entrez Gene ID", "sequence")
  //.join(idsTable, $"id" === $"Entrez Gene ID")
  //.select("feature_id", "sequence")
  guideTable.show

  /* ... new cell ... */

  val neuroStem: Set[String] = Set("sox2", "foxg1", "pou3f2", "sox5", "sox9", "myt1l", "ascl1", "neurod1", "lmx1a")
  val neuronsMogrify = Set("cux2","sox2","hes6")
  val dopamin = Set("ascl1", "sox2", "nr4a2", "lmx1a", "pitx3", "neurog2")
  val stemCells = Set("nanog", "sox2", "pou3f2", "sirt1")
  val other = Set("tert", "shh")
  val ourGeneNames = neuroStem ++ neuronsMogrify ++ stemCells ++ dopamin ++ other
  val lowGenes = ourGeneNames.map(_.toLowerCase)
  lowGenes

  /* ... new cell ... */

  val genes = features.genes.filterByGeneName(g=>lowGenes.contains(g.toLowerCase))
  val genesById = genes.rdd.map{g=>
    (g.getGeneId, g.getAttributes.get("gene_name"), g.region)
  }

  val byRegion: RDD[(ReferenceRegion, (String, String))] = genesById.map{ case (id, name, region) => (region, (id, name)) }

  val ourGenes = genome.extractRegions(byRegion.keys.collect().toList).join(byRegion).map{
    case (r, (sequence, (id, name))) => (id, name,/* r,*/ sequence)
  }

  val df = ourGenes.toDS()
    .withColumnRenamed("_1", "gene_id")
    .withColumnRenamed("_2", "gene_name")
    .withColumnRenamed("_3", "guide")

  df.show

  /* ... new cell ... */

  val all = df.join(idsTable, "$")
  all.show

  /* ... new cell ... */

  import comp.bio.aging.playground.extensions._


  val transcriptNames = features.transcripts.filterByGeneName(g=>lowGenes.contains(g.toLowerCase)).rdd.map{g=>
    (g.getTranscriptId, g.getAttributes.get("gene_name"))
  }

  val tnf = transcriptNames.toDF("transcript", "gene_name").distinct()
  val tfs = tnf.join(refSeq, tnf("transcript") === refSeq("transcript_id")).select($"refseq_1", $"refseq_2", $"transcript_id", $"gene_name")
  tfs.show

  /* ... new cell ... */

  val extracted = genome.extractBeforeTranscripts(features, lowGenes, 200).cache()
  extracted.count

  /* ... new cell ... */

  extracted.collect.toMap

  /* ... new cell ... */

  val geneContains = "brn2" //put a string that should be part of the gene's name
  val searchContainsResult = features.rdd.filter(f=>
    f.getFeatureType =="gene" &&
      f.getAttributes.containsKey("gene_name") &&
      f.getAttributes.get("gene_name").toLowerCase.contains(geneContains.toLowerCase)).collect
  searchContainsResult

  /* ... new cell ... */

  import comp.bio.aging.crispr._
  val cas9 = new Cas9ADAM()
  val guidomeYCas9 = cas9.filterByGC(cas9.guidome(chrY, true)) //include 5 before, pam.transform(rdd => rdd.cache) //let's cache to avoid recomputations
  guidomeYCas9.rdd.count //number of Cas9 gRNAs on Y chromosome
  val guidomeYCas9Ext = cas9.filterByGC(cas9.guidome(chrY, true, 5, 2)) //include 5 before,
    .transform(rdd => rdd.cache) //let's cache to avoid recomputations
  val cpf1 = new Cpf1ADAM
  val guidomeYCpf1 = cpf1.filterByGC(cpf1.guidome(chrY, true))
    .transform(rdd => rdd.cache) //let's cache to avoid recomputations
  guidomeYCpf1.rdd.count() //number of cpf1 gRNAs on Y chromosome
  //guidomeYCpf1.getTotalLength

  /* ... new cell ... */

  val geneName = "Sirt1" //change me with the gene you love!
  val searchResult = features.rdd.filter(f=>
    f.getFeatureType =="gene" &&
      f.getAttributes.containsKey("gene_name") &&
      f.getAttributes.get("gene_name").toLowerCase == geneName.toLowerCase).collect
  searchResult

  /* ... new cell ... */

  val geneContains = "Tert" //put a string that should be part of the gene's name
  val searchContainsResult = features.rdd.filter(f=>
    f.getFeatureType =="gene" &&
      f.getAttributes.containsKey("gene_name") &&
      f.getAttributes.get("gene_name").toLowerCase.contains(geneContains.toLowerCase)).collect
  searchContainsResult

  /* ... new cell ... */

  val geneName2 = "Tert" //put gene name for which you want to find transcripts here
  val searchTranscriptResult = features.rdd.filter(f=>
    f.getFeatureType =="transcript" &&
      f.getAttributes.containsKey("gene_name") &&
      f.getAttributes.get("gene_name").toLowerCase == geneName2.toLowerCase).collect
  searchTranscriptResult

  /* ... new cell ... */
}
