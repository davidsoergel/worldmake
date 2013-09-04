package worldmake

import scala.concurrent._
import scala.collection.{GenMap, mutable, GenSet}
import worldmake.storage.Storage
import com.typesafe.scalalogging.slf4j.Logging
import ExecutionContext.Implicits.global
import worldmake.storage.Identifier
import scala.util.{Success, Failure}
import scalax.file.Path
import org.joda.time.DateTime
import worldmake.WorldMakeConfig._
import scala.util.Failure
import worldmake.storage.Identifier
import scala.Some
import scala.util.Success
import scalax.io.Resource
import java.io.File
import scala.sys.process.{Process, ProcessLogger}
import java.util.UUID

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

trait FutureDerivationStrategy extends Logging {
  def resolveOne[T](d: Derivation[T]): Future[Successful[T]]
 def systemExecution:SystemExecutionStrategy 
  // even if a derivation claims to be deterministic, it may still be derived multiple times (e.g. to confirm identical results)

  // def deriveMulti(howMany: Integer): GenSet[Provenance[T]] = (0 to howMany).toSet.par.map((x: Int) => derive)
}



/*
trait AwaitFutureDerivationStrategy extends DerivationStrategy with Logging {

  val fallback : FutureDerivationStrategy
  def resolveOne[T](d:Derivation[T],args:ResolvedArguments) = {
    logger.warn("Blocking: awaiting result of future: " + d.description)
    Await.result(fallback.resolveOne(d),Duration.Inf)
  }
  
}

trait LocalDerivationStrategy extends DerivationStrategy {
  
}
*/

trait FallbackFutureDerivationStrategy extends FutureDerivationStrategy {
  val fallback: FutureDerivationStrategy
  def systemExecution = fallback.systemExecution
}

trait CachingFutureDerivationStrategy extends FallbackFutureDerivationStrategy {

  def printTree[T](d: Derivation[T], prefix: String): String = {
    d.shortId + prefix + " [" + statusString(d) + "] " + d.description
  }

  def printTree[A, T](d: DerivableDerivation[T], prefix: String): String = {
    printTree(d.asInstanceOf[Derivation[T]], prefix) + "\n" + d.dependencies.map(printTree(_, prefix + WorldMakeConfig.prefixIncrement)).mkString("\n")
  }

  def statusLine[T](d: Derivation[T]) = f"${d.shortId}%8s [ ${statusString(d)}%22s ] ${d.summary}%40s : ${d.description}%-40s"

  private def provenances[T](d: Derivation[T]): GenSet[Provenance[T]] = Storage.provenanceStore.getDerivedFrom(d.derivationId)

  def successes[T](d: Derivation[T]): GenSet[Successful[T]] =
    provenances(d).collect({
      case x: Successful[T] => x
    })

  def failures[T](d: Derivation[T]) = provenances(d).filter(_.status == ProvenanceStatus.Failure)

  def statusString[T](d: Derivation[T]): String = {
    if (successes(d).size > 0) {
      ProvenanceStatus.Success + " (" + successes(d).size + " variants)"
    } else if (failures(d).size > 0) {
      ProvenanceStatus.Failure + " (" + failures(d).size + " failures)"
    }
    else "no status" // todo print other statuses
  }

  // todo super tricky: if the computation is already running in another thread / in another JVM / on another machine, wait for it instead of starting a new one.

  def resolveOne[T](d: Derivation[T]): Future[Successful[T]] = synchronized {
    successes(d).toSeq.headOption.map(x => Future.successful(x)).getOrElse(
      RunningDerivations.getAs(d.derivationId)
        .getOrElse({
        val result = fallback.resolveOne(d)
        RunningDerivations.put(d.derivationId, result)
        result onComplete {
          case Success(t) => RunningDerivations.remove(d.derivationId)
          case Failure(t) => {} // leave failures in place so they don't run redundantly // RunningDerivations.remove(d.derivationId)
        }
        result
      }))
  }
}

object RunningDerivations {
  private val running: mutable.Map[Identifier[Derivation[_]], Future[Successful[_]]] = new mutable.HashMap[Identifier[Derivation[_]], Future[Successful[_]]] with mutable.SynchronizedMap[Identifier[Derivation[_]], Future[Successful[_]]]

  def put(id: Identifier[Derivation[_]], f: Future[Successful[_]]) {
    synchronized {
      running.put(id, f)
    }
  }

  def getAs[T](id: Identifier[Derivation[T]]): Option[Future[Successful[T]]] = synchronized {
    running.get(id).map(_.asInstanceOf[Future[Successful[T]]])
  }

  def remove(id: Identifier[Derivation[_]]) {
    synchronized {
      running.remove(id)
    }
  }
}


class ComputeFutureDerivationStrategy(upstreamStrategy: FutureDerivationStrategy, val systemExecution:SystemExecutionStrategy) extends FutureDerivationStrategy {
  @throws(classOf[FailedDerivationException])
  def resolveOne[T](d: Derivation[T]): Future[Successful[T]] = d.deriveFuture(upstreamStrategy)

}

