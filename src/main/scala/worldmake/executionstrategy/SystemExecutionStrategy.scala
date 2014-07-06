/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.executionstrategy

import scalax.file.Path
import scala.concurrent.Future
import worldmake.storage.Identifier
import worldmake._

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait SystemExecutionStrategy {
  def apply(pr: BlockedProvenance[ManagedPath], scriptF: Future[Successful[String]], env: Future[Iterable[(String, Successful[Any])]]): Future[Successful[ManagedPath]]

}
