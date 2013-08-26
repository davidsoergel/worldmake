package fuseexample

import worldmake._
import scalax.file.Path
import worldmake.lib.MercurialWorld


object EmergeWorld extends MercurialWorld {
  lazy val reposRequestedVersions: Map[String, (String, String)] = Map("edu.umass.cs.iesl.stopwords.chinese" ->(("default", "latest")),"edu.umass.cs.iesl.stopwords.english" ->(("default", "latest")))

  val stopwords: Stopwords = new Stopwords() {
    val chinese = synchronized { new StopwordsFile(workingDirs("edu.umass.cs.iesl.stopwords.chinese") , "ch.txt") }
    val english = synchronized { new StopwordsFile(workingDirs("edu.umass.cs.iesl.stopwords.english") , "en.txt") }
  }
  
  val allChinesePatentsTokenized = {
    val byYear = for (year <- 1985 to 2011) yield {
      val x = new ChinesePatentsTokenized( ConstantDerivation(ConstantProvenance(StringArtifact(year.toString))))
        x.rawInput
    }
    new TraversableDerivation(byYear.seq)
  }
}




case class StopwordsFile(dir: ExternalPathDerivation, relativePath : String) {
  def file = dir.resolveOne.artifact.value / relativePath
}


trait Stopwords {
  def english : StopwordsFile
  def chinese : StopwordsFile
}

/*
object StopwordsX extends Stopwords {

  val chineseStopwords: Stopwords = new Stopwords() {
    def chinese = workingDirs("edu.umass.cs.iesl.stopwords.chinese") / "ch.txt"
    def english = workingDirs("edu.umass.cs.iesl.stopwords.english") / "en.txt"
  }

  def chinese = {
    val directory = MercurialWorkspaces.get("edu.umass.cs.iesl.stopwords.chinese").resolve
    (directory / "ch.txt")
  }

  def english = {
    val directory = MercurialWorkspaces.get("edu.umass.cs.iesl.stopwords.english").resolve.value
    (directory / "en.txt").toFile
  }
}
*/
