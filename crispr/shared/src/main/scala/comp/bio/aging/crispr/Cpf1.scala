package comp.bio.aging.crispr

/**
  * Created by antonkulaga on 3/21/17.
  */
class Cpf1 extends CRISPR{
  lazy val pam = "TTTV"

  lazy val forwardCut: Int = 18
  lazy val reverseCut: Int = 23
  lazy val guideEnd = 23 //just after PAM

  def crRNA: String = ??? //TODO: put 42-nt short CRISPR RNA
}
