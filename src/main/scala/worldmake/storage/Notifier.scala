package worldmake.storage

import worldmake.{Successful, Provenance, Derivation}
import scala.concurrent._
import scala.collection.mutable

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Notifier {
  def getSuccessful[T](id : Identifier[Derivation[T]]) : Future[Successful[T]]
}


trait CallbackNotifier extends Notifier {
  def announceDone[T](p:Successful[T])
  // ** what to do on failure?
}

class BasicCallbackNotifier extends Notifier {
  val waitingFor:mutable.Map[Identifier[Derivation[_]],Promise[Successful[_]]] = new mutable.HashMap[Identifier[Derivation[_]],Promise[Successful[_]]] with mutable.SynchronizedMap[Identifier[Derivation[_]],Promise[ Successful[_]]]


  def announceDone[T](pr:Successful[T]) = {
    for(p<-waitingFor.get(pr.derivationId)) {
      p success pr
    }
  }


  def getSuccessful[T](id : Identifier[Derivation[T]]) : Future[Successful[T]] = {
    val p = waitingFor.get(id).getOrElse({
      val np = promise[Provenance[T] with Successful[T]]()
      waitingFor.put (id , np.asInstanceOf[Promise[Successful[_]]])
      np
    })
    p.future.asInstanceOf[Future[Successful[T]] ]
  }
  
}


trait PollingNotifier extends Notifier
