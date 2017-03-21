package comp.bio.aging.crispr

/**
  * Created by antonkulaga on 3/20/17.
  */
class Cas9 extends CRISPR {
  lazy val pam = "NGG"

  lazy val forwardCut: Int = -3
  lazy val reverseCut: Int = -3

}
