package comp.bio.aging.crispr

import org.scalatest.{Matchers, WordSpec}
import comp.bio.aging.playground.extensions.stringSeqExtensions._

class CRISPRTest extends WordSpec with Matchers{

  "CAS9" should {

    val dnas = Vector( "ACAGCTGATCTCCAGATATGACCATGGGTT", "CAGCTGATCTCCAGATATGACCATGGGTTT", "CCAGAAGTTTGAGCCACAAACCCATGGTCA")

    "cut in a right place" in {
      val dna = dnas.head
      val cas = new Cas9
      val pams = cas.searchesOf(dna, cas.pam,  0, 20, 0)
      pams shouldEqual List(24, 25)
      val cuts = cas.cuts(pams).map(_._1)
      dna.splitAt(cuts.head) shouldEqual ("ACAGCTGATCTCCAGATATGA", "CCATGGGTT")
      dna.splitAt(cuts.tail.head) shouldEqual ("ACAGCTGATCTCCAGATATGAC", "CATGGGTT")

      val dna2 = dnas(1)
      val pams2 = cas.searchesOf(dna2, cas.pam, 0, 20, 0)
      val cuts2 = cas.cuts(pams2).map(_._1)
      dna2.splitAt(cuts2.head) shouldEqual ("CAGCTGATCTCCAGATATGA", "CCATGGGTTT")
      dna2.splitAt(cuts2.tail.head) shouldEqual ("CAGCTGATCTCCAGATATGAC", "CATGGGTTT")

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
      val cuts = cpf1.cuts(pams)
      cuts shouldEqual  List((22,27))
      //"TTTACAGTGACGTCGGTTAGGA"
      dna1.splitAt(cuts.head._1) shouldEqual("TTTACAGTGACGTCGGTTAGGA", "CACTG")
      dnaComp.splitAt(cuts.head._2) shouldEqual ("TTTACAGTGACGTCGGTTAGGACACTG".complement, "")
    }
  }

}
