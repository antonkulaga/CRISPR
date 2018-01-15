import comp.bio.aging.playground.trees.SimpleTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD



val spark = SparkSession
  .builder()
  .appName("Homology search")
  .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
spark.sparkContext.setLogLevel("WARN")

import org.apache.spark.SparkContext

val sparkContext: SparkContext

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession

import org.bdgenomics.adam.rdd.ADAMContext._

import comp.bio.aging.playground.extensions._

import org.apache.spark.sql._

import scala.util.Try

import org.apache.spark.storage.StorageLevel

object Cells {
  import org.apache.spark.{SparkConf, SparkContext}

  import org.apache.spark.sql.SparkSession

  import org.bdgenomics.adam.rdd.ADAMContext._

  import comp.bio.aging.playground.extensions._

  import org.apache.spark.sql._

  import scala.util.Try

  import org.apache.spark.storage.StorageLevel



  val spark = SparkSession

    .builder()

    .appName("Homology search")

    .config("spark.executor.cores","8")

    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  /* ... new cell ... */

  def readTSV(path: String) = spark.read.option("sep", "\t").csv(path)



  val base = "hdfs://namenode/pipelines"

  val human = base + "/HUMAN/GRCh38/27"

  val genomeAdam = human + "/genome.adam"

  val chrYAdam = human + "/chrY.adam"


  val cho = base + "/CHO"
  val choAdam = cho +"/genome_CHO.adam"
  val genomeCHO = sparkContext.loadParquetContigFragments(choAdam)



  /* ... new cell ... */

  val genome = sparkContext.loadParquetContigFragments(genomeAdam)

  val contigNames = genome.sequences.records.map(_.name)

  contigNames


  /* ... new cell ... */

  import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD

  def saveHuman(frags: NucleotideContigFragmentRDD,name: String) = {

    val path = if(name.startsWith("/")) human + name else human + "//" + name

    frags.saveAsParquet(path)

    path

  }

  /* ... new cell ... */

  val guidomePamAdam =       human + "/guidomePam.adam"

  val guidomeNoPamAdam =     human + "/guidomeNoPam.adam"

  val guidomeChrYPamAdam = human + "/guidomeChrYPam.adam"

  val guidomeChrYNoPamAdam =   human + "/guidomeChrYNoPam.adam"


  /* ... new cell ... */

  import comp.bio.aging.crispr._

  val cas9 = new Cas9ADAM()

  //val chrY = genome.transform(rdd=>rdd.filter(_.getContigName=="chrY"))

  //saveHuman(chrY, "/chrY.adam")



  //val guidomePam = cas9.guidome(genome, true)

  //val guidomeNoPam = cas9.guidome(genome, false)

  val guidomePam = sparkContext.loadParquetContigFragments(guidomePamAdam)

  val guidomeNoPam = sparkContext.loadParquetContigFragments(guidomeNoPamAdam)


  val chrY = sparkContext.loadParquetContigFragments(chrYAdam)

  //val chrYNoPam = cas9.guidome(chrY, false)

  //val chrYPam = cas9.guidome(chrY, true)

  val chrYPam = sparkContext.loadParquetContigFragments(guidomeChrYPamAdam)

  val chrYNoPam = sparkContext.loadParquetContigFragments(guidomeChrYNoPamAdam)

  /* ... new cell ... */


  val guidomePamCHO = cas9.guidome(genomeCHO, true)

  val guidomeNoPamCHO = cas9.guidome(genomeCHO, false)

  val guidomePamAdamCHO =   cho + "/guidomePamCHO.adam"
  val guidomeNoPamAdamCHO =   cho + "/guidomeNoPamCHO.adam"
  guidomePamCHO.saveAsParquet(guidomePamAdamCHO)
  guidomeNoPamCHO.saveAsParquet(guidomeNoPamAdamCHO)


  /* ... new cell ... */


  def countRepeats(fragments: NucleotideContigFragmentRDD, moreThan: Int = 2): RDD[(String, Int)] = {
    val mp = fragments.rdd.map(fr=>fr.getSequence->1)
    mp.reduceByKey(_ + _).filter(f=>f._2 >= moreThan)
  }

  val yReps = countRepeats(chrYNoPam, 10).cache
  yReps.count

  /* ... new cell ... */

  def countRepeatsByContig(fragments: NucleotideContigFragmentRDD, moreThan: Int = 5): RDD[((String, String), Int)] = {
    val mp = fragments.rdd.map(fr=>(fr.getSequence, fr.getContigName)->1)
    mp.reduceByKey(_ + _).filter(f=>f._2 >= moreThan)
  }

  /* ... new cell ... */



  val noY = guidomeNoPam.transform(rdd=>rdd.filter(c=>c.getContigName!="chrY"))
  val noYrepeats5 = countRepeatsByContig(noY, 5).persist(StorageLevel.MEMORY_AND_DISK)
  noYrepeats5.count
  /* ... new cell ... */
  yReps.keys.subtract(noYrepeats5.keys.map(_._1))
  /* ... new cell ... */

  val choReps5 = countRepeatsByContig(guidomeNoPamCHO, 5).persist(StorageLevel.MEMORY_AND_DISK)
  choReps5.count()
  /* ... new cell ... */
  yReps.keys.subtract(choReps5.keys.map(_._1))
  /* ... new cell ... */





  //(chrY.rdd.count, chrYNoPam.rdd.count, chrYPam.rdd.count, guidomePam.rdd.count)

  /* ... new cell ... */

  import comp.bio.aging.playground.trees.SimpleTree

  val tree = SimpleTree(chrYNoPam)
  val repsY = tree.repeatsOfMin(20, 20)


  /* ... new cell ... */

  //saveHuman(chrYPam, "/guidomeChrYPam.adam")

  //saveHuman(chrYNoPam, "/guidomeChrYNoPam.adam")

  //saveHuman(guidomePam, "/guidomePam.adam")

  //saveHuman(guidomeNoPam, "/guidomeNoPam.adam")

  /* ... new cell ... */

  // four unique loci that contained 8, 15, 21 and 33 repeat sequences (locus #1, 2, 3 and 4)

  // https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5424063/#S1



  val locus1 = "GACCAGGGAGGAGGAGACAT" //8

  val locus2 = "GAGAGTTGAGTCTGTACTGT" //15

  val locus3 = "GTAAGGTTCAGACTCTGGCT" //21

  val locus4 = "GTGTAGACAGTGGAGCAGCT" //33

  /* ... new cell ... */

  def repeats(frag: NucleotideContigFragmentRDD, str: String) = {

    frag.rdd.filter{
      f => f.getSequence == str//f.getSequence.contains(str)
      }.map(f=>f.getStart -> f.getSequence)

  }

  //def slideRepeats(guidomeNoPam: NucleotideContigFragmentRDD, guides: String) = {
  //  guidomeNoPam.filter(f=>f)
  //}



  //repeats(guidomePam, locus1)


  /* ... new cell ... */

  guidomeNoPam.rdd.count

  /* ... new cell ... */

  val reps = repeats(guidomeNoPam, locus1).collect

  reps

  /* ... new cell ... */

  def findRepeats(guidome: NucleotideContigFragmentRDD, minRepeats: Int) = {

    guidome.rdd.map(g=>g.getSequence -> g.getStart).aggregateByKey(List.empty[Long])(

      { case (list, long) => long::list },

      { case (a, b) => a ++ b }

    ).filter{

      case (str, list) => list.lengthCompare(minRepeats) >= 0

    }

  }

  /* ... new cell ... */

  val reps = findRepeats(guidomeNoPam, 10)

  reps.count

  /* ... new cell ... */

  import better.files._

  import java.io.{File => JFile}

  val pipelines = better.files.File("/pipelines")

  val indexes = pipelines / "indexes"

  /* ... new cell ... */

  val human = indexes / "HUMAN" / "27"

  human.list.toList

  /* ... new cell ... */



  /* ... new cell ... */
}
