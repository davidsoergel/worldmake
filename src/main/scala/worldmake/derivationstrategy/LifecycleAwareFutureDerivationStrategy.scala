package worldmake.derivationstrategy

import worldmake.storage.{StoredProvenances, Identifier}
import worldmake._
import scala.concurrent.Future
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait LifecycleAwareFutureDerivationStrategy extends FallbackFutureDerivationStrategy {

  val tracker: LifecycleTracker

  // todo super tricky: if the computation is already running in another thread / in another JVM / on another machine, wait for it instead of starting a new one.

  // perf multiple calls to tracker produce new DB queries every time

  def resolveOne[T](d: Derivation[T]): Future[Successful[T]] = synchronized {
    val id = d.derivationId
    val track = tracker.track(id)
    track.getSuccessAs.map(x => Future.successful(x)).getOrElse(
      track.getPotentialSuccessesAs
        .getOrElse({
        if (track.hasFailure && !WorldMakeConfig.retryFailures) {
          throw FailedDerivationException("Aborting due to failure of " + d.derivationId, d.derivationId)
        }
        val result = fallback.resolveOne(d) // trust that the underlying strategy updates the storage
        // RunningDerivations.put(d.derivationId, result)
        /*result onComplete {
          case Success(t) => RunningDerivations.remove(d.derivationId)
          case Failure(t) => {} // leave failures in place so they don't run redundantly // RunningDerivations.remove(d.derivationId)
        }*/
        result
      }))
  }
}

class LifecycleTracker(notifier: Notifier) extends Logging {

  //def put(id: Identifier[Derivation[_]], f: Future[Successful[_]])
  //def getAs[T](id: Identifier[Derivation[T]]): Option[Future[Successful[T]]]
  //def remove(id: Identifier[Derivation[_]])

  class Track[T](id: Identifier[Derivation[T]]) {
    val sp = StoredProvenances(id)

    def hasFailure: Boolean = sp.failures.nonEmpty

    def getSuccessAs: Option[Successful[T]] = sp.successes.toSeq.headOption

    def getRunningAs: Option[Future[Successful[T]]] = {
      val rr = sp.running.toSeq
      if (rr.size > 1) {
        logger.warn(rr.size + " running jobs for Derivation " + id + "!")
      }
      rr.headOption.map(_ => notifier.request(id))
    }

    def getPotentialSuccessesAs: Option[Future[Successful[T]]] = {
      val rr = StoredProvenances(id).potentialSuccesses.toSeq
      if (rr.size > 1) {
        logger.warn(rr.size + " potentially successful jobs for Derivation " + id + "!")
      }
      rr.headOption.map(_ => notifier.request(id))
    }

  }

  def track[T](id: Identifier[Derivation[T]]) = new Track(id)

  def printTree[T](d: Derivation[T], prefix: String): String = {
    d.shortId + prefix + " [" + StoredProvenances(d.derivationId).statusString + "] " + d.description
  }

  def printTree[A, T](d: DerivableDerivation[T], prefix: String): String = {
    printTree(d.asInstanceOf[Derivation[T]], prefix) + "\n" + d.dependencies.map(printTree(_, prefix + WorldMakeConfig.prefixIncrement)).mkString("\n")
  }

  def statusLine[T](d: Derivation[T]) = f"${d.shortId}%8s [ ${StoredProvenances(d.derivationId).statusString}%22s ] ${d.summary}%40s : ${d.description}%-40s"


}

/**
 * Track running derivations only within this JVM.
 */
/*
object MemoryLifecycleTracker extends LifecycleTracker {
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
*/
