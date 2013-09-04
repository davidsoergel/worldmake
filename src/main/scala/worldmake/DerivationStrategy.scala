package worldmake

import scala.concurrent._
import scala.collection.{mutable, GenSet}
import worldmake.storage.Storage
import com.typesafe.scalalogging.slf4j.Logging
import ExecutionContext.Implicits.global
import worldmake.storage.Identifier
import scala.util.{Success, Failure}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
/*
trait DerivationStrategy {
  @throws(classOf[FailedDerivationException])
  def resolveOne[T](d:Derivation[T]): Successful[T]
  def resolveArguments[A<:UnresolvedArguments0](args:A):ResolvedArguments0

  /*
  override final def resolveOneNew: Provenance[T] = {
    val cached = Storage.provenanceStore.getDerivedFrom(derivationId)
    // assert that the type parameter is OK
    if (cached.nonEmpty) cached.map(_.asInstanceOf[Provenance[T]]) else Set(derive)
  }
*/

  //def numSuccess = provenances.count(_.output.nonEmpty)

  //def numFailure = provenances.count(_.output.isEmpty)

  /*
  override def status: DerivationStatus = if (numSuccess > 0) Cached
  else if (dependencies.exists(x => {
    val s = x.status
    s == Blocked || s == Pending || s == Error
  })) Blocked
  else if (failures.count > 0) Error
  else Pending
*/

}
*/
trait FutureDerivationStrategy extends Logging {
  def resolveOne[T](d: Derivation[T]): Future[Successful[T]]

  // even if a derivation claims to be deterministic, it may still be derived multiple times (e.g. to confirm identical results)

  // def deriveMulti(howMany: Integer): GenSet[Provenance[T]] = (0 to howMany).toSet.par.map((x: Int) => derive)


  //lazy val resolveOneFuture: Future[Successful[T]] = future { resolveOne }


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
trait QsubDerivationStrategy extends FutureDerivationStrategy {

}


trait CachingFutureDerivationStrategy extends FutureDerivationStrategy {
  //extends DerivationStrategy {

  //val fallback : DerivationStrategy

  // the "best" status found among the Provenances
  // def status: DerivationStatuses.DerivationStatus

  // def statusString[T](d:Derivation[T]): String //= status.name


  def printTree[T](d: Derivation[T], prefix: String): String = {
    d.shortId + prefix + " [" + statusString(d) + "] " + d.description
  }

  def printTree[A, T](d: DerivableDerivation[T], prefix: String): String = {
    printTree(d.asInstanceOf[Derivation[T]], prefix) + "\n" + d.dependencies.map(printTree(_, prefix + WorldMakeConfig.prefixIncrement)).mkString("\n")
  }

  def statusLine[T](d: Derivation[T]) = {
    //val stat = f" [ ${statusString(d)}%22s ] "
    //d.shortId + stat + d.summary + d.description
    f"${d.shortId}%8s [ ${statusString(d)}%22s ] ${d.summary}%40s : ${d.description}%-40s"
  }

  /*
    def resolveOne[T](d:Derivation[T]): Successful[T] = synchronized {
      successes(d).toSeq.headOption.getOrElse({
        val p = fallback.resolveOne(d)
        if (p.status == ProvenanceStatus.Success) p else throw new FailedDerivationException
      })
    }
  */

  private def provenances[T](d: Derivation[T]): GenSet[Provenance[T]] = Storage.provenanceStore.getDerivedFrom(d.derivationId)

  def successes[T](d: Derivation[T]): GenSet[Successful[T]] = // provenances.filter(_.status == ProvenanceStatus.Success)
    provenances(d).collect({
      case x: Successful[T] => x
    })

  // .filter(_.status == Success)
  def failures[T](d: Derivation[T]) = provenances(d).filter(_.status == ProvenanceStatus.Failure)


  def statusString[T](d: Derivation[T]): String = {
    if (successes(d).size > 0) {
      ProvenanceStatus.Success + " (" + successes(d).size + " variants)"
    } else if (failures(d).size > 0) {
      ProvenanceStatus.Failure + " (" + failures(d).size + " failures)"
    }
    else "no status" // todo print other statuses
  }

  val fallback: FutureDerivationStrategy

  // todo super tricky: if the computation is already running in another thread / in another JVM / on another machine, wait for it instead of starting a new one.

  def resolveOne[T](d: Derivation[T]): Future[Successful[T]] = synchronized {
    successes(d).toSeq.headOption.map(x => Future.successful(x)).getOrElse(
      RunningDerivations.getAs(d.derivationId)
        .getOrElse({
        val result = fallback.resolveOne(d)

        /*val result = f.map(p => {
          RunningDerivations.remove(d.derivationId)
          if (p.status == ProvenanceStatus.Constant || p.status == ProvenanceStatus.Success) { 
            p 
          } else {
            throw new FailedDerivationException("Impossible: successful provenance has status " + p.status)
            /*p match {
              case dp : DerivedProvenance[T] =>
                throw new FailedDerivationException(dp.logString)
              case _ => throw new FailedDerivationException("Impossible failure of ")
            }*/
          }
        })*/

        RunningDerivations.put(d.derivationId, result)
        result onComplete {
          case Success(t) => RunningDerivations.remove(d.derivationId)
          case Failure(t) => RunningDerivations.remove(d.derivationId)
        }
        result
      }))
  }
}

object RunningDerivations {
  private val running: mutable.Map[Identifier[Derivation[_]], Future[Successful[_]]] = new mutable.HashMap[Identifier[Derivation[_]], Future[Successful[_]]] with mutable.SynchronizedMap[Identifier[Derivation[_]], Future[Successful[_]]]

  def put(id: Identifier[Derivation[_]], f: Future[Successful[_]]) {
    running.put(id, f)
  }

  def getAs[T](id: Identifier[Derivation[T]]): Option[Future[Successful[T]]] = running.get(id).map(_.asInstanceOf[Future[Successful[T]]])

  def remove(id: Identifier[Derivation[_]]) {
    running.remove(id)
  }
}

/*
trait CachingFutureDerivationStrategy extends CachingDerivationStrategy with FutureDerivationStrategy {

  val fallback : FutureDerivationStrategy
  
  // todo super tricky: if the computation is already running in another thread / in another JVM / on another machine, wait for it instead of starting a new one.

  def resolveOneFuture[T](d:Derivation[T]): Future[Successful[T]] = synchronized {
    successes(d).toSeq.headOption.map(x => future {
      x
    }).getOrElse({
      val f = fallback.resolveOne(d)
      f.map(p => {
        if (p.status == ProvenanceStatus.Success) p else throw new FailedDerivationException
      })
    })
  }
}*/

class LocalFutureDerivationStrategy(upstreamStrategy: FutureDerivationStrategy) extends FutureDerivationStrategy {
  @throws(classOf[FailedDerivationException])
  def resolveOne[T](d: Derivation[T]): Future[Successful[T]] = d.deriveFuture(upstreamStrategy)

  /*future {
  val resolved : Future[Successful[T]] = upstreamStrategy.resolveArguments(d.dependencies) //.resolved()
  val result : Future[Successful[T]] = resolved.map(d)
  result
}*/
}

/*
class LocalDerivationStrategy(upstreamStrategy: DerivationStrategy) extends DerivationStrategy {
  @throws(classOf[FailedDerivationException])
  def resolveOne[A<:UnresolvedArguments,T](d: DerivableDerivation[A,T]):Successful[T] = {
    val resolved : ResolvedArguments  = upstreamStrategy.resolveArguments(d.dependencies) //.resolved()
    val result : Successful[T] = d(resolved)
    result
  }
}
*/
