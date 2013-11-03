package worldmake


import com.typesafe.scalalogging.slf4j.Logging
import java.util.UUID
import worldmake.storage.{ManagedPathArtifact, Storage, Identifier}
import scala.collection.{GenTraversable, GenSet, GenMap}
import scala.concurrent.{ExecutionContext, Future}

import ExecutionContext.Implicits.global
import worldmake.cookingstrategy.CookingStrategy
import scalax.io.Resource
import org.joda.time.DateTime

//import java.lang.ProcessBuilder.Redirect

import worldmake.WorldMakeConfig._

//import scala.collection.JavaConversions._

import scalax.file.Path

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

object SystemRecipe {
  // "toEnvironmentString" is not a method of the Derivation trait because the Any->String conversion may differ by 
  // context (at least, eg., # of sig figs, or filename vs file contents, etc.)
  // For that matter, what if it differs for different arguments of the same type? 
  /* def toEnvironmentString[T](x: Artifact[T]): String = x match {
     case f: ExternalPathArtifact => f.abspath
     case f: GenTraversableArtifact[T] => f.artifacts.map((x: Artifact[_]) => toEnvironmentString(x)).mkString(" ")
     //case f:GenTraversableArtifact => f.artifacts.map(toEnvironmentString).mkString(" ")
     case f => f.value.toString
   }*/

}

class SystemRecipe(val script: Recipe[String], namedDependencies: GenMap[String, Recipe[_]]) extends DerivableRecipe[ManagedPath] with Logging {

  lazy val recipeId = {
    val dependencyInfos: Seq[String] = namedDependencies.par.map({
      case (k, v) => k.toString + v.recipeId.s
    }).toSeq.seq.sorted
    Identifier[Recipe[ManagedPath]](WMHashHex(script.recipeId.s + dependencyInfos.mkString("")))
  }

  lazy val longDescription = "EXECUTE(" + script.shortId + "): " + script.longDescription

  lazy val dependencies = namedDependencies.values.toSet + script

  def deriveFuture(implicit upstreamStrategy: CookingStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[ManagedPath]](UUID.randomUUID().toString), recipeId)
    val reifiedScriptF = upstreamStrategy.cookOne(script)
    val reifiedDependenciesF = Future.traverse(namedDependencies.keys.seq)(k => FutureUtils.futurePair(upstreamStrategy, (k, namedDependencies(k))))
    val result = upstreamStrategy.systemExecution(pr, reifiedScriptF, reifiedDependenciesF)
    result
  }

}

class DumpToFileRecipe(val s: Recipe[GenTraversable[String]]) extends DerivableRecipe[ManagedPath] with Logging {

  lazy val recipeId = {
    Identifier[Recipe[ManagedPath]](WMHashHex("filedump(" + s.recipeId.s + ")"))
  }

  lazy val longDescription = "filedump(" + s.shortId + "): " + s.longDescription

  lazy val dependencies : GenSet[Recipe[_]] = Set(s)

  def deriveFuture(implicit upstreamStrategy: CookingStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[ManagedPath]](UUID.randomUUID().toString), recipeId)
    val reifiedStringF: Future[Successful[GenTraversable[String]]] = upstreamStrategy.cookOne(s)
    val result = {
      for (reifiedString: Successful[GenTraversable[String]] <- reifiedStringF
      ) yield {

        // see also LocalExecutionStrategy.
        // this dump must be local because the data to be written is just in this JVM.
        val outputId: Identifier[ManagedPath] = Storage.fileStore.newId
        val outputPath: Path = Storage.fileStore.getOrCreate(outputId)
        val out = Resource.fromFile(outputPath.toRealPath().path)
        val ss: GenTraversable[String] = reifiedString.output.value
        ss.map(out.write)
        
        val now = new DateTime()
        InstantCompletedProvenance[ManagedPath](
          Identifier[Provenance[ManagedPath]](UUID.randomUUID().toString),
          recipeId,
          Set(reifiedString),
          Map.empty,
          now,
          now,
          now,
          new MemoryWithinJvmRunningInfo,
          now,
          0,
          None,
          Map.empty,
          ManagedPathArtifact(ManagedPath(outputId)))
      }
    }
  result
  }

}

object FutureUtils {
  //def futurePair[T](kv:(String,Derivation[T]))(implicit strategy: FutureDerivationStrategy):Future[(String,Successful[T])] = kv._2.deriveFuture.map(v=>kv._1->v)
  def futurePair[T](upstreamStrategy: CookingStrategy, kv: (String, Recipe[T]))(implicit strategy: CookingStrategy): Future[(String, Successful[T])] = kv match {
    case (key, deriv) => {
      val f = upstreamStrategy.cookOne(deriv)
      f.map(s => (key, s))
    }
  }

}
