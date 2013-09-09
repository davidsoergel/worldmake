package worldmake.lib

import worldmake._
import scalax.file.Path
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.{GenTraversable, GenMap}
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import java.util.UUID
import org.joda.time.DateTime
import java.nio.file.{FileSystems, Files}
import java.nio.file
import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global
import worldmake.derivationstrategy.FutureDerivationStrategy

import StringInterpolationDerivation._

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object FileDerivations extends Logging {

  val fsTraverse = new IdentifiableFunction2[TypedPath, String, TypedPath]("fsTraverse", {
    (a: TypedPath, b: String) => new UnknownTypedPath(a.path / b)
  })

  val countLines = new IdentifiableFunction1[TextFile, Int]("countLines", (tf: TextFile) => {
    logger.debug("Counting lines in " + tf.path.fileOption.get.toString)
    val lines: Seq[String] = Source.fromFile(tf.path.fileOption.get)(scala.io.Codec.UTF8).getLines().toSeq
    val result = lines.length
    logger.debug(s"Found $result lines")
    result
  })

  def sedFile(sedFile:TypedPathDerivation[TextFile],input:TypedPathDerivation[TextFile]) = sys"sed -f ${sedFile} < ${input} > $${out}"
  def sedExpr(sedExpr:Derivation[String],input:TypedPathDerivation[TextFile]) = sys"sed -e '${sedExpr}' < ${input} > $${out}"

}

class AssemblyDerivation(namedDependencies: GenMap[String, Derivation[TypedPath]]) extends DerivableDerivation[TypedPath] with Logging {

  lazy val derivationId = {
    val dependencyInfos: Seq[String] = namedDependencies.map({
      case (k, v) => k.toString + v.derivationId.s
    }).toSeq.seq.sorted
    Identifier[Derivation[TypedPath]](WMHashHex(dependencyInfos.mkString("")))
  }

  def description = "Assembly"

  def dependencies = namedDependencies.values.toSet

  /*protected def derive = {
    val reifiedDependencies = namedDependencies.par.mapValues(_.resolveOne)
    deriveWithArgs(reifiedDependencies)
  }*/


  def deriveFuture(implicit upstreamStrategy: FutureDerivationStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[TypedPath]](UUID.randomUUID().toString), derivationId)
    val reifiedDependenciesF = Future.traverse(namedDependencies.keys.seq)(k=>FutureUtils.futurePair((k,namedDependencies(k))))
    val result = for( reifiedDependencies <- reifiedDependenciesF
    ) yield deriveWithArgs(pr.pending(Set.empty,reifiedDependencies.toMap), reifiedDependencies.toMap)
    
    result

  }
  
  private def deriveWithArgs(pr: PendingProvenance[TypedPath],reifiedDependencies:GenMap[String,Successful[TypedPath]]): CompletedProvenance[TypedPath]  = synchronized {
    val prs = pr.running(new MemoryWithinJvmRunningInfo)
    
    try{
    val outputPath: Path = fileStore.newPath
    outputPath.createDirectory(createParents = true, failIfExists = true)
    reifiedDependencies.map({
      case (n, v) => {
        val target = FileSystems.getDefault.getPath(v.output.value.abspath)
        val link: file.Path = FileSystems.getDefault.getPath((outputPath / n).path)
        Files.createSymbolicLink(link, target)
      }
    })

    val result : Artifact[TypedPath] = TypedPathArtifact(TypedPathMapper.map("assembly", outputPath))
    prs.completed(0, None, Map.empty, result)
    }
    catch {
      case t: Throwable => {
        val prf = prs.failed(1, None, Map.empty)
        logger.debug("Error in AssemblyDerivation: ", t) // todo better log message
        throw FailedDerivationException("Failed AssemblyDerivation", prf, t)
      }
    }
    
  }
}
