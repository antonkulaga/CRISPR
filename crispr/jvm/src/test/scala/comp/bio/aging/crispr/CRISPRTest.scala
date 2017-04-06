package comp.bio.aging.crispr

import org.scalatest.{Matchers, WordSpec}
import comp.bio.aging.playground.extensions.stringSeqExtensions._

class CRISPRTest extends WordSpec with Matchers{

  "CAS9" should {

    val dnas = Vector( "ACAGCTGATCTCCAGATATGACCATGGGTT", "CAGCTGATCTCCAGATATGACCATGGGTTT", "CCAGAAGTTTGAGCCACAAACCCATGGTCA")

    "cut in a right place" in {
      val dna = dnas.head
      val cas9 = new Cas9
      val pams = cas9.searchesOf(dna, cas9.pam,  0, 20, 0)
      pams shouldEqual List(24, 25)
      cas9.pamSearch(dna) shouldEqual pams
      cas9.guideSearchIn(dna, false).map(_._1 + Math.abs(cas9.guideEnd)) shouldEqual pams
      cas9.guideSearchIn(dna, true).map(_._1 + Math.abs(cas9.guideEnd)) shouldEqual pams
      val cuts = cas9.cuts(pams).map(_._1)
      dna.splitAt(cuts.head) shouldEqual ("ACAGCTGATCTCCAGATATGA", "CCATGGGTT")
      dna.splitAt(cuts.tail.head) shouldEqual ("ACAGCTGATCTCCAGATATGAC", "CATGGGTT")
      val cutsByPam = cas9.cutByPams(pams, dna)
      cutsByPam.map(_._2._2) shouldEqual cuts
      cutsByPam.head._1 shouldEqual "CTGATCTCCAGATATGACCA"
      cutsByPam.tail.head._1 shouldEqual "TGATCTCCAGATATGACCAT"


      val dna2 = dnas(1)
      val pams2: List[Int] = cas9.searchesOf(dna2, cas9.pam, 0, 20, 0)
      cas9.pamSearch(dna2) shouldEqual pams2
      cas9.guideSearchIn(dna2, false).map(_._1 + Math.abs(cas9.guideEnd)) shouldEqual pams2
      cas9.guideSearchIn(dna2, true).map(_._1 + Math.abs(cas9.guideEnd)) shouldEqual pams2
      val cuts2 = cas9.cuts(pams2).map(_._1)
      dna2.splitAt(cuts2.head) shouldEqual ("CAGCTGATCTCCAGATATGA", "CCATGGGTTT")
      dna2.splitAt(cuts2.tail.head) shouldEqual ("CAGCTGATCTCCAGATATGAC", "CATGGGTTT")
      val cutsByPam2 = cas9.cutByPams(pams2, dna2)
      cutsByPam2.map(_._2._2) shouldEqual cuts2
      cutsByPam2.head._1 shouldEqual "CTGATCTCCAGATATGACCA"
      cutsByPam2.tail.head._1 shouldEqual "TGATCTCCAGATATGACCAT"
    }

  }

  "Cpf1" should {

    val dnas = Vector( "TTTACAGTGACGTCGGTTAGGACACTG")

    "cut in a right place" in {
      val cpf1 = new Cpf1
      val dna1 = dnas.head
      val dnaComp = dna1.complement
      cpf1.searchesOf(dna1 + cpf1.pam + "AGC", cpf1.pam, 0 , 0, 23).length shouldEqual 1
      val pams = cpf1.searchesOf(dna1, cpf1.pam, 0, 0 , 23)
      cpf1.pamSearch(dna1) shouldEqual pams
      cpf1.guideSearchIn(dna1, false).map(_._1 - cpf1.pam.length) shouldEqual pams
      cpf1.guideSearchIn(dna1, true).map(_._1) shouldEqual pams
      val cuts = cpf1.cuts(pams)
      cuts shouldEqual  List((22,27))

      dna1.splitAt(cuts.head._1) shouldEqual("TTTACAGTGACGTCGGTTAGGA", "CACTG")
      dnaComp.splitAt(cuts.head._2) shouldEqual ("TTTACAGTGACGTCGGTTAGGACACTG".complement, "")
      val cutsByPams = cpf1.cutByPams(pams, dna1)
      cutsByPams.head._2 shouldEqual cuts.head
      cutsByPams.head._1 shouldEqual "CAGTGACGTCGGTTAGGACACTG"
    }
  }

}
