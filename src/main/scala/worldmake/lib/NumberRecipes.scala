/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.lib

import worldmake.IdentifiableFunction2

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object NumberRecipes {
  // todo: figure out how to clean up the user syntax
  val divide = new IdentifiableFunction2[Int, Int, Int]("divide", {
    (a: Int, b: Int) => a / b
  })
}
