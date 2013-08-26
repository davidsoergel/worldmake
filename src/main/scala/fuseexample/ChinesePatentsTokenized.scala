package fuseexample

import worldmake._
import scalax.file.Path
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging


/**
 * Every production rule is a function from some Derivations to a new Derivation.
 * Or, equivalently, a Class with Derivations as constructor arguments and Derivations as publicly readable outputs.
 */

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

trait EmergeTopicClusterInput extends Logging {

  //implicit val codec = scala.io.Codec.UTF8
  
  def rawInput: Derivation[Path]
 
  protected def base_stop_words : StopwordsFile
  
  // these must be protected because they are not Derivations (they could be recast that way, but it's not worth the trouble)
  protected val num_docs: Integer = {
    val path = base_stop_words.file
    logger.debug("Reading stopwords file from " + path.fileOption.get.toString) 
    val lines :Seq[String] = Source.fromFile(path.fileOption.get)(scala.io.Codec.UTF8).getLines().toSeq
    val result = lines.length
    logger.debug(s"Found $result stopwords")
    result
  }
  
  // defaults, may be overridden
  val num_clusters: Derivation[Integer] = ConstantDerivation(ConstantProvenance(new MemoryIntegerArtifact(num_docs / 15)))
  val num_topics: Derivation[Integer] = ConstantDerivation(ConstantProvenance(new MemoryIntegerArtifact(40)))
}

class ChinesePatentsTokenized(year: Derivation[String]) extends EmergeTopicClusterInput {
  private val patfile: Derivation[Path] =  ConstantDerivation(ConstantProvenance(ExternalPathArtifact(Path.fromString("/scratch/bbn/nagarwal/chipatff/raw_input_" + year.resolveOne.artifact.value + ".txt"))))
  
  protected override def base_stop_words = EmergeWorld.stopwords.chinese

  private val inputCleaner: Derivation[String] =  ConstantDerivation(ConstantProvenance(StringArtifact(
    """
      |cat ${patfile} | tr '\t' ' ' | grep -v '\w  $$' > ${raw_input}
    """.stripMargin)))

  def rawInput: Derivation[Path] = new SystemDerivation(inputCleaner, Map("patfile" -> patfile))

  /*
  patfile = /scratch/bbn/nagarwal/chipatff/raw_input_${year}.txt

base_stop_words = inputs/stopwords/ch.txt
token_regex := '[\p{L}\p{M}]+'

num_docs=$(shell cat $(raw_input) | wc -l)
num_clusters=$(shell echo $(num_docs)/15 | bc)
num_topics=40

${raw_input} : ${patfile}
	cat ${patfile} | tr '\t' ' ' | grep -v '\w  $$' > ${raw_input}

   */
}

