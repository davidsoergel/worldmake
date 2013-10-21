package worldmake.lib

import worldmake._
import scalax.file.Path
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.GenMap
import worldmake.storage._
import java.util.UUID
import java.nio.file.{FileSystems, Files}
import java.nio.file
import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global
import worldmake.cookingstrategy.CookingStrategy

import StringInterpolationRecipe._
import WorldMakeConfig.WMHashHex
import worldmake.storage.Identifier

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object FileRecipes extends Logging {

/*  val fsTraverse = new IdentifiableFunction2[PathArtifact[PathReference], String, PathArtifact]("fsTraverse", {
    (a: PathArtifact[PathReference], b: String) => new PathArtifact(new PathReference() {val path = a.path / b} )
  })

  val children = new IdentifiableFunction1[PathArtifact[PathReference], Seq[PathArtifact]]("fsChildren", {
    (a: PathArtifact[PathReference]) => {
      val result: Seq[PathArtifact] = a.children.toSeq
      result
    }
  })*/

  val countLines = new IdentifiableFunction1[TextFile, Int]("countLines", (tf: TextFile) => {
    logger.debug("Counting lines in " + tf.path.fileOption.get.toString)
    val lines: Seq[String] = Source.fromFile(tf.path.fileOption.get)(scala.io.Codec.UTF8).getLines().toSeq
    val result = lines.length
    logger.debug(s"Found $result lines")
    result
  })

  def sedFile(sedFile: TypedPathRecipe[TextFile], input: TypedPathRecipe[TextFile]) = sys"sed -f ${sedFile} < ${input} >  $${out}"

  def sedExpr(sedExpr: Recipe[String], input: TypedPathRecipe[TextFile]) = sys"sed -e '${sedExpr}' < ${input} >  $${out}"

}

class AssemblyRecipe(namedDependencies: GenMap[String, Recipe[PathReference]]) extends DerivableRecipe[ManagedPath] with Logging {

  lazy val recipeId = {
    val dependencyInfos: Seq[String] = namedDependencies.par.map({
      case (k, v) => k.toString + v.recipeId.s
    }).toSeq.seq.sorted
    Identifier[Recipe[ManagedPath]](WMHashHex(dependencyInfos.mkString("")))
  }

  def longDescription = "Assembly: " + namedDependencies.toList.map({
    case (k, v) => s"$k <- ${v.description}"
  }).mkString("\n             ")

  def dependencies = namedDependencies.values.toSet

  /*protected def derive = {
    val reifiedDependencies = namedDependencies.par.mapValues(_.resolveOne)
    deriveWithArgs(reifiedDependencies)
  }*/


  def deriveFuture(implicit upstreamStrategy: CookingStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[ManagedPath]](UUID.randomUUID().toString), recipeId)
    val reifiedDependenciesF = Future.traverse(namedDependencies.keys.seq)(k => {
      FutureUtils.futurePair(upstreamStrategy, (k, namedDependencies(k)))
    })
    val result = for (reifiedDependencies <- reifiedDependenciesF
    ) yield {
      deriveWithArgs(pr.pending(Set.empty, reifiedDependencies.toMap), reifiedDependencies.toMap)
    }

    result

  }

  private def deriveWithArgs(pr: PendingProvenance[ManagedPath], reifiedDependencies: GenMap[String, Successful[PathReference]]): CompletedProvenance[ManagedPath] = synchronized {
    val prs = pr.running(new MemoryWithinJvmRunningInfo)

    try {
      val outputId: Identifier[ManagedPath] = Storage.fileStore.newId
      val outputPath: Path = Storage.fileStore.getOrCreate(outputId)
      outputPath.createDirectory(createParents = true, failIfExists = true)
      reifiedDependencies.map({
        case (n, v) => {
          val target = FileSystems.getDefault.getPath(v.output.value.abspath)
          val link: file.Path = FileSystems.getDefault.getPath((outputPath / n).path)
          Files.createSymbolicLink(link, target)
        }
      })

      //val result : Artifact[TypedPath] = TypedPathArtifact(TypedPathMapper.map("assembly", outputPath))
      prs.completed(0, None, Map.empty, ManagedPathArtifact(ManagedPath(outputId)))
    }
    catch {
      case t: Throwable => {
        val prf = prs.failed(1, None, Map.empty)
        logger.debug("Error in AssemblyRecipe: ", t) // todo better log message
        throw FailedRecipeException("Failed AssemblyRecipe", prf, t)
      }
    }

  }
}
