package worldmake.cookingstrategy

import worldmake._
import scala.concurrent._
import scala.collection.mutable
import worldmake.storage.StoredProvenances
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import worldmake.storage.Identifier
import worldmake.storage.Identifier
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Notifier {
  def request[T](id: Identifier[Recipe[T]]): Future[Successful[T]]
  def shutdown() {}
}

trait CallbackNotifier extends Notifier {
  def announceDone(p: Successful[_])

  def announceFailed(p: FailedProvenance[_])

  def announceCancelled(p: CancelledProvenance[_])

  // ** what to do on failure?
}

class BasicCallbackNotifier extends CallbackNotifier with Logging {
  val waitingFor: mutable.Map[Identifier[Recipe[_]], Promise[Successful[_]]] = new mutable.HashMap[Identifier[Recipe[_]], Promise[Successful[_]]] with mutable.SynchronizedMap[Identifier[Recipe[_]], Promise[Successful[_]]]

  def announceDone(pr: Successful[_]) = {
    for (p <- waitingFor.get(pr.recipeId)) {
      waitingFor.remove(pr.recipeId)
      p success pr
    }
  }

  def announceFailed(p: FailedProvenance[_]) = announceFailedOrCancelled(p.recipeId)
  def announceCancelled(p: CancelledProvenance[_]) = announceFailedOrCancelled(p.recipeId)

  private def announceFailedOrCancelled(id:Identifier[Recipe[Any]]) = {

    // one provenance may have failed, but the downstream provenances could just use a different one
    // see if there are any other successes...
    val sp = StoredProvenances(id)
    if (sp.successes.isEmpty && sp.potentialSuccesses.isEmpty) {
      val t = FailedRecipeException("Failure detected: no potential success for recipe: " + id, id)
      for (p <- waitingFor.get(id)) {
        if(p.isCompleted) {
          logger.error("Promise was already completed; ignoring failure of recipe " + id)
        p failure t
        }
      }
    }
  }
  
  // the job must actually be started somewhere else.  This just promises to notify.
  def request[T](id: Identifier[Recipe[T]]): Future[Successful[T]] = {
    val p = waitingFor.get(id).getOrElse({
      val np = promise[Successful[T]]()
      waitingFor.put(id, np.asInstanceOf[Promise[Successful[_]]])
      np
    })
    p.future.asInstanceOf[Future[Successful[T]]]
  }
}

class PollingNotifier(pollingActions: Seq[PollingAction]) extends BasicCallbackNotifier {

  val actorSystem = ActorSystem()
  val scheduler = actorSystem.scheduler
  val task = new Runnable {
    def run() {
      // figure out which derivations we're looking for, and see what provenances exist for those

      val sps = waitingFor.keys.map(StoredProvenances[Any])

      for (a <- pollingActions) {
        a(sps, PollingNotifier.this)
      }

    }
  }
  implicit val executor = actorSystem.dispatcher

  scheduler.schedule(
    initialDelay = Duration(5, TimeUnit.SECONDS),
    interval = Duration(10, TimeUnit.SECONDS),
    runnable = task)
  
  override def shutdown() {
    actorSystem.shutdown()
  }
}


trait PollingAction {
  def apply(sps: Iterable[StoredProvenances[_]], notifier: CallbackNotifier)
}

object DetectSuccessPollingAction extends PollingAction {
  def apply(sps: Iterable[StoredProvenances[_]], notifier: CallbackNotifier) {
    for (sp <- sps)
      sp.successes.toSeq.headOption.map(pr => notifier.announceDone(pr))
  }
}


object DetectFailedPollingAction extends PollingAction {
  def apply(sps: Iterable[StoredProvenances[_]], notifier: CallbackNotifier) {
    for (sp <- sps) {
      if (sp.successes.isEmpty && sp.potentialSuccesses.isEmpty)
        notifier.announceFailed(sp.failures.head) // just pick one, it doesn't matter...
    }
  }
}
