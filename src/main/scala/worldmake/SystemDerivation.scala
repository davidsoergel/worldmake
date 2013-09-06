package worldmake


import com.typesafe.scalalogging.slf4j.Logging
import scala.sys.process.{ProcessLogger, Process}
import scalax.io.Resource
import java.util.UUID
import org.joda.time.DateTime
import worldmake.storage.Identifier
import scala.collection.{GenTraversable, GenMap}
import scala.concurrent.{ExecutionContext, Future}

import ExecutionContext.Implicits.global
import scala.collection.immutable.Queue
import worldmake.derivationstrategy.FutureDerivationStrategy

//import java.lang.ProcessBuilder.Redirect

import WorldMakeConfig._

//import scala.collection.JavaConversions._

import scalax.file.Path
import java.io.File

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

object SystemDerivation {
  // "toEnvironmentString" is not a method of the Derivation trait because the Any->String conversion may differ by 
  // context (at least, eg., # of sig figs, or filename vs file contents, etc.)
  // For that matter, what if it differs for different arguments of the same type? 
  def toEnvironmentString[T](x: Artifact[T]): String = x match {
    case f: ExternalPathArtifact => f.abspath
    case f: GenTraversableArtifact[T] => f.artifacts.map((x: Artifact[_]) => toEnvironmentString(x)).mkString(" ")
    //case f:GenTraversableArtifact => f.artifacts.map(toEnvironmentString).mkString(" ")
    case f => f.value.toString
  }

}

class SystemDerivation(val script: Derivation[String], namedDependencies: GenMap[String, Derivation[_]]) extends DerivableDerivation[Path] with Logging {

  override def queue: Queue[Derivation[_]] = {
    val deps = dependencies.seq.toSeq.flatMap(_.queue)
    Queue[Derivation[_]](deps: _*).distinct.enqueue(this)
  }
  
  lazy val derivationId = {
    val dependencyInfos: Seq[String] = namedDependencies.map({
      case (k, v) => k.toString + v.derivationId.s
    }).toSeq.seq.sorted
    Identifier[Derivation[Path]](WMHashHex(script.derivationId.s + dependencyInfos.mkString("")))
  }

  def description = "EXECUTE(" + script.shortId + "): " + script.description

  val dependencies = namedDependencies.values.toSet + script

  def deriveFuture(implicit upstreamStrategy: FutureDerivationStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[Path]](UUID.randomUUID().toString), derivationId)
    val reifiedScriptF = upstreamStrategy.resolveOne(script)
    val reifiedDependenciesF = Future.traverse(namedDependencies.keys.seq)(k=>FutureUtils.futurePair((k,namedDependencies(k))))   
    val result = upstreamStrategy.systemExecution(pr, reifiedScriptF,reifiedDependenciesF)   
    result 
  }
  
}


object FutureUtils {

   def futurePair[T](kv:(String,Derivation[T]))(implicit strategy: FutureDerivationStrategy):Future[(String,Successful[T])] = kv._2.deriveFuture.map(v=>kv._1->v)
}
