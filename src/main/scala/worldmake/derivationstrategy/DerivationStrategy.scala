package worldmake.derivationstrategy

import scala.concurrent._
import scala.collection.mutable
import com.typesafe.scalalogging.slf4j.Logging
import worldmake._
import worldmake.executionstrategy.SystemExecutionStrategy
import worldmake.storage.Identifier

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

trait FutureDerivationStrategy extends Logging {
  def resolveOne[T](d: Derivation[T]): Future[Successful[T]]

  def systemExecution: SystemExecutionStrategy

  // even if a derivation claims to be deterministic, it may still be derived multiple times (e.g. to confirm identical results)

  // def deriveMulti(howMany: Int): GenSet[Provenance[T]] = (0 to howMany).toSet.par.map((x: Int) => derive)
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



class ComputeFutureDerivationStrategy(upstreamStrategy: FutureDerivationStrategy, val systemExecution: SystemExecutionStrategy) extends FutureDerivationStrategy {
  @throws(classOf[FailedDerivationException])
  def resolveOne[T](d: Derivation[T]): Future[Successful[T]] = d.deriveFuture(upstreamStrategy)

}

