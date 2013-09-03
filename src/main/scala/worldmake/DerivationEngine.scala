package worldmake

import scala.concurrent.Future

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait DerivationEngine {
def derive[T](d:Derivation[T]) : Successful[T]
}

trait FutureDerivationEngine {
  def deriveFuture[T](d:Derivation[T]) : Future[Successful[T]]
}

trait LocalDerivationEngine extends DerivationEngine {
  
}

trait QsubDerivationEngine extends FutureDerivationEngine {
  
}
