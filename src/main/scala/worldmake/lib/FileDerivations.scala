package worldmake.lib

import worldmake._
import scalax.file.Path
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.GenMap
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import java.util.UUID
import org.joda.time.DateTime
import java.nio.file.{FileSystems, Files}
import java.nio.file
import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object FileDerivations extends Logging {

  val fsTraverse = new IdentifiableFunction2[Path, String, Path]("fsTraverse", {
    (a: Path, b: String) => a / b
  })

  val countLines = new IdentifiableFunction1[TextFile, Integer]("countLines", (tf: TextFile) => {
    logger.debug("Counting lines in " + tf.path.fileOption.get.toString)
    val lines: Seq[String] = Source.fromFile(tf.path.fileOption.get)(scala.io.Codec.UTF8).getLines().toSeq
    val result = lines.length
    logger.debug(s"Found $result lines")
    result
  })

}

class AssemblyDerivation(namedDependencies: GenMap[String, Derivation[Path]]) extends DerivableDerivation[Path] {

  lazy val derivationId = {
    val dependencyInfos: Seq[String] = namedDependencies.map({
      case (k, v) => k.toString + v.derivationId.s
    }).toSeq.seq.sorted
    Identifier[Derivation[Path]](WMHashHex(dependencyInfos.mkString("")))
  }

  def description = "Assembly"

  def dependencies = namedDependencies.values.toSet

  protected def derive = {
    val reifiedDependencies = namedDependencies.par.mapValues(_.resolveOne)
    deriveWithArgs(reifiedDependencies)
  }


  def deriveFuture = {

    val reifiedDependenciesF = Future.traverse(namedDependencies.keys.seq)(k=>FutureUtils.futurePair((k,namedDependencies(k))))
    for( reifiedDependencies <- reifiedDependenciesF
    ) yield deriveWithArgs( reifiedDependencies.toMap)

  }
  
  private def deriveWithArgs(reifiedDependencies:GenMap[String,Successful[Path]]) = synchronized {

    val startTime = DateTime.now()
    val outputPath: Path = fileStore.newPath
    outputPath.createDirectory(createParents = true, failIfExists = true)
    reifiedDependencies.map({
      case (n, v) => {
        val target = FileSystems.getDefault.getPath(v.artifact.value.path)
        val link: file.Path = FileSystems.getDefault.getPath((outputPath / n).path)
        Files.createSymbolicLink(link, target)
      }
    })

    val result = ExternalPathArtifact(outputPath)

    val endTime = DateTime.now()

    SuccessfulProvenance(Identifier[Provenance[Path]](UUID.randomUUID().toString),
      derivationId = AssemblyDerivation.this.derivationId,
      status = ProvenanceStatus.Success,
      derivedFromNamed = reifiedDependencies,
      derivedFromUnnamed = Set.empty,
      startTime = startTime,
      endTime = endTime,
      statusCode = None,
      log = None,
      output = Some(result))

  }
}
