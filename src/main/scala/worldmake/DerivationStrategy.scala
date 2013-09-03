package worldmake

import scala.concurrent._
import scala.collection.GenSet
import worldmake.storage.Storage
import worldmake.{UnresolvedArgumentsNamed, UnresolvedArguments2, FailedDerivationException}
import scala.concurrent.duration.Duration
import com.typesafe.scalalogging.slf4j.Logging
import scalaz.Monad
import scalaz.Id

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
  def resolveOne[T](d:Derivation[T]): Future[Successful[T]]
  
  def resolveArguments(args:UnresolvedArguments0):Future[ResolvedArguments0] = future { new ResolvedArguments0 }
  def resolveArguments[A](args:UnresolvedArguments1[A]):Future[ResolvedArguments1[A]] = resolveOne(args.a) map (new ResolvedArguments1(_))
  def resolveArguments[A,B](args:UnresolvedArguments2[A,B]):Future[ResolvedArguments2[A,B]]
  def resolveArguments[A,B,C](args:UnresolvedArguments3[A,B,C]):Future[ResolvedArguments3[A,B,C]]
  def resolveArguments[A](args:UnresolvedArgumentsSet[A]):Future[ResolvedArgumentsSet[A]]
  def resolveArguments[A](args:UnresolvedArgumentsNamed[A]):Future[ResolvedArgumentsNamed[A]]

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


trait CachingDerivationStrategy { //extends DerivationStrategy {
  
  //val fallback : DerivationStrategy

  // the "best" status found among the Provenances
  // def status: DerivationStatuses.DerivationStatus

  // def statusString[T](d:Derivation[T]): String //= status.name

  def printTree[T](d:Derivation[T], prefix: String): String = {
    d.shortId + prefix + " [" + statusString(d) + "] " + d.description
  }

  def printTree[A,T](d:DerivableDerivation[A,T], prefix: String): String = {
    printTree(d.asInstanceOf[Derivation[T]],prefix) + "\n" + d.dependencies.derivations.map(printTree(_,prefix + WorldMakeConfig.prefixIncrement)).mkString("\n")
  }

  
  def statusLine[T](d:Derivation[T]) = {
    val stat = f" [ ${statusString(d)}%22s ] "
    d.shortId + stat + d.description
  }


  def resolveOne[T](d:Derivation[T]): Successful[T] = synchronized {
    successes(d).toSeq.headOption.getOrElse({
      val p = fallback.resolveOne(d)
      if (p.status == ProvenanceStatus.Success) p else throw new FailedDerivationException
    })
  }

  private def provenances[T](d:Derivation[T]): GenSet[Provenance[T]] = Storage.provenanceStore.getDerivedFrom(d.derivationId)

  def successes[T](d:Derivation[T]): GenSet[Successful[T]] = // provenances.filter(_.status == ProvenanceStatus.Success)
    provenances(d).collect({
      case x: Successful[T] => x
    })

  // .filter(_.status == Success)
  def failures[T](d:Derivation[T]) = provenances(d).filter(_.status == ProvenanceStatus.Failure)


  def statusString[T](d:Derivation[T]): String = {
    if (successes(d).size > 0) {
      ProvenanceStatus.Success + " (" + successes(d).size + " variants)"
    } else if (failures(d).size > 0) {
      ProvenanceStatus.Failure + " (" + failures(d).size + " failures)"
    }
    else "no status" // todo print other statuses
  }

}

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
}

class LocalFutureDerivationStrategy(upstreamStrategy: FutureDerivationStrategy) extends FutureDerivationStrategy {
  @throws(classOf[FailedDerivationException])
  def resolveOne[T](d: DerivableDerivation[UnresolvedArguments, T]):Future[Successful[T]] = future {
    val resolved : Future[ResolvedArguments] = upstreamStrategy.resolveArguments(d.dependencies) //.resolved()
    val result : Future[Successful[T]] = resolved.map(d(_))
    result
  }
}

class LocalDerivationStrategy(upstreamStrategy: DerivationStrategy) extends DerivationStrategy {
  @throws(classOf[FailedDerivationException])
  def resolveOne[A<:UnresolvedArguments,T](d: DerivableDerivation[A,T]):Successful[T] = {
    val resolved : ResolvedArguments  = upstreamStrategy.resolveArguments(d.dependencies) //.resolved()
    val result : Successful[T] = d(resolved)
    result
  }
}
