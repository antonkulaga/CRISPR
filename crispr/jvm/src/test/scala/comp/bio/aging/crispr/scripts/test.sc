import coursier.maven.MavenRepository
interp.repositories() ++= Seq(MavenRepository("https://dl.bintray.com/comp-bio-aging/main/"))
@
import $ivy.`comp.bio.aging:crispr_2.11:0.0.7`
import comp.bio.aging.crispr._

val cas = new Cas9



println("Hello")
