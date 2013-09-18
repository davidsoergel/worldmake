package worldmake.cookingstrategy

import scala.concurrent._
import scala.collection.mutable
import com.typesafe.scalalogging.slf4j.Logging
import worldmake._
import worldmake.executionstrategy.SystemExecutionStrategy
import worldmake.storage.Identifier

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

trait CookingStrategy extends Logging {
  def cookOne[T](d: Recipe[T]): Future[Successful[T]]

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

class NotAvailableCookingStrategy extends CookingStrategy {
  def cookOne[T](d: Recipe[T]) = throw new CookingException("Recipes cannot be cooked in this configuration")

  def systemExecution = throw new CookingException("Recipes cannot be cooked in this configuration")
}

class CookingException(message:String) extends Exception(message)

trait FallbackCookingStrategy extends CookingStrategy {
  val fallback: CookingStrategy

  def systemExecution = fallback.systemExecution
}



class ComputeNowCookingStrategy(upstreamStrategy: CookingStrategy, val systemExecution: SystemExecutionStrategy) extends CookingStrategy {
  @throws(classOf[FailedRecipeException])
  def cookOne[T](d: Recipe[T]): Future[Successful[T]] = d.deriveFuture(upstreamStrategy)

}

