package worldmake.executionstrategy

import scalax.file.Path
import scala.concurrent.Future
import worldmake.storage.Identifier
import worldmake.{BlockedProvenance, Successful, Derivation}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait SystemExecutionStrategy {
  def apply(pr: BlockedProvenance[Path], scriptF: Future[Successful[String]], env: Future[Iterable[(String, Successful[Any])]]): Future[Successful[Path]]

}
