package worldmake


import com.typesafe.scalalogging.slf4j.Logging
import java.util.UUID
import worldmake.storage.Identifier
import scala.collection.GenMap
import scala.concurrent.{ExecutionContext, Future}

import ExecutionContext.Implicits.global
import scala.collection.immutable.Queue
import worldmake.cookingstrategy.CookingStrategy

//import java.lang.ProcessBuilder.Redirect

import WorldMakeConfig._

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

  lazy val  recipeId = {
    val dependencyInfos: Seq[String] = namedDependencies.par.map({
      case (k, v) => k.toString + v.recipeId.s
    }).toSeq.seq.sorted
    Identifier[Recipe[ManagedPath]](WMHashHex(script.recipeId.s + dependencyInfos.mkString("")))
  }

  lazy val   longDescription = "EXECUTE(" + script.shortId + "): " + script.longDescription

  lazy val  dependencies = namedDependencies.values.toSet + script

  def deriveFuture(implicit upstreamStrategy: CookingStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[ManagedPath]](UUID.randomUUID().toString), recipeId)
    val reifiedScriptF = upstreamStrategy.cookOne(script)
    val reifiedDependenciesF = Future.traverse(namedDependencies.keys.seq)(k => FutureUtils.futurePair(upstreamStrategy,(k, namedDependencies(k))))
    val result = upstreamStrategy.systemExecution(pr, reifiedScriptF, reifiedDependenciesF)
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
