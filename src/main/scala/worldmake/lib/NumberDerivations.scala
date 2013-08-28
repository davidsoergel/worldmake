package worldmake.lib

import worldmake.IdentifiableFunction2

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object NumberDerivations {
  // todo: figure out how to clean up the user syntax
  val divide = new IdentifiableFunction2[Integer, Integer, Integer]("divide", {
    (a: Integer, b: Integer) => a / b
  })
}