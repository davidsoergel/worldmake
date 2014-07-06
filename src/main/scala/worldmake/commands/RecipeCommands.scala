/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.commands

import worldmake._
import worldmake.cookingstrategy.LifecycleAwareCookingStrategy
import com.typesafe.scalalogging.slf4j.Logging

import scalax.io.Output
import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration.Duration

import ExecutionContext.Implicits.global

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class RecipeCommands[T](world: World, strategy: LifecycleAwareCookingStrategy, out: Output, recipe: Recipe[T]) extends Logging {
  def showRecipe() {
    out.write(strategy.tracker.recipeStatusLine(recipe))
    val multi = new MultiProvenanceCommands(out, ProvenanceFinder(recipe))
    multi.showProvenances()
  }

  def showRecipeQueue() = {
    recipe.queue.filterNot(_.isInstanceOf[ConstantRecipe[Any]]).map(x => out.write(strategy.tracker.recipeStatusLine(x) + "\n"))
  }
  
  def verifyRecipeInputs() = {
    recipe.queue.filter(_.isInstanceOf[ConstantRecipe[Any]]).map(x => out.write(strategy.tracker.verifyRecipeInputs(x) + "\n"))
  }

  def showRecipeDeps() = {
    recipe match {
      case d: DerivableRecipe[_] => d.dependencies.map(x => out.write(strategy.tracker.recipeStatusLine(x) + "\n"))
      case _ => out.write(s"Recipe ${recipe.recipeId} has no dependencies.")
    }
  }

  def showRecipeTree() = {
    out.write(strategy.tracker.printTree(recipe, ""))
  }

  def makeRecipe() = {
    //val target = args(1)

    //val strategy = getStrategy(withNotifiers = true)

    //val derivationId = symbolTable.getProperty(target) 
    //val derivationArtifact = Storage.artifactStore.get(derivationId)
    //val recipe: Recipe[_] = world(target)
    val result = strategy.cookOne(recipe)
    result onComplete {
      case Success(x) => {
        out.write("Done: " + x.provenanceId)
        out.write("Value: " + x.output.value.toString)
      }
      case Failure(t) => {
        logger.error("Failed to make recipe.", t)
        throw t
      }
    }

    Await.result(result, Duration.Inf)

  }
}
