package worldmake.example

import java.io.File
import scala.collection.GenTraversable
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake._
import scalax.file.Path

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class Example {

}

object ConcatenateFiles {
  def apply(xs: Traversable[Derivation[File]]) : Derivation[Path] = {
    val script = ConstantDerivation(ConstantProvenance(StringArtifact(
      """
        |#!/bin/sh
        |cat $* >> $out
      """.stripMargin)))
    new SystemDerivation(script, Map("args"->new TraversableDerivation(xs)))

  }
}
