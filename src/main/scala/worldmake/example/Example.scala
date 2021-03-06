/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.example

import java.io.File
import scala.collection.GenTraversable
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake._
import scalax.file.Path

import ConstantRecipe._
/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class Example {

}

// todo update

object ConcatenateFiles {
  def apply(xs: Traversable[Recipe[File]]) : Recipe[ManagedPath] = {
    val script : Recipe[String] =
      """
        |#!/bin/sh
        |cat $* >> $out
      """.stripMargin
    new SystemRecipe(script, Map("args"->new TraversableRecipe(xs)))

  }
}
