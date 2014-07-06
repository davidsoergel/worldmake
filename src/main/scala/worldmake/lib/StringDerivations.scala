/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.lib

import worldmake._
import java.util.UUID
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.StringUtils._
import scala.concurrent.{ExecutionContext, Future}

import ExecutionContext.Implicits.global
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.cookingstrategy.CookingStrategy

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class StringInterpolationRecipe(sc: StringContext, args: Seq[Recipe[Any]]) extends DerivableRecipe[String] with Logging {

  def deriveFuture(implicit upstreamStrategy: CookingStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[String]](UUID.randomUUID().toString), recipeId)
    val resolvedProvenancesF: Future[Seq[Successful[Any]]] = Future.sequence(args.map(upstreamStrategy.cookOne))
    val result = for (resolvedProvenances <- resolvedProvenancesF
    ) yield deriveWithArgs(pr.pending(resolvedProvenances.toSet, Map.empty), resolvedProvenances)
    result
  }


  def deriveWithArgs(pr: PendingProvenance[String], resolvedProvenances: Seq[Successful[Any]]): Provenance[String] with Successful[String] = {
    val prs = pr.running(new MemoryWithinJvmRunningInfo)
    try {
      val result = {
        val resolvedArgs = resolvedProvenances.map(_.output.environmentString) //.value)

        // this stripMargin takes effect after interpolation; see https://github.com/scala/scala/pull/1655 for alternative
        val r = StringArtifact(sc.s(resolvedArgs: _*).stripMargin)
        r
      }
      prs.completed(0, None, Map.empty, result)
    }
    catch {
      case t: Throwable => {
        val prf = prs.failed(1, None, Map.empty)
        logger.debug("Error in StringInterpolationRecipe: ", t) // todo better log message
        throw FailedRecipeException("Failed StringInterpolationRecipe", prf, t)
      }
    }
  }

  private val template: String = sc.parts.mkString("???").stripMargin.maskNewlines

  lazy val recipeId = Identifier[Recipe[String]](WMHashHex(template + args.par.map(_.recipeId).mkString))

  def longDescription = {
    val argIds = args.map(x => "${" + x.shortDesc + "}")

    // this stripMargin takes effect after interpolation; see https://github.com/scala/scala/pull/1655 for alternative
    val r = sc.s(argIds: _*).stripMargin
    "Interpolate: " + r.replace("\n", "\\n") //.limitAtWhitespace(80, "...")
  }

  def dependencies = args.toSet
}

object StringInterpolationRecipe {

  // Note: We extends AnyVal to prevent runtime instantiation.  See 
  // value class guide for more info.
  implicit class StringInterpolationRecipeHelper(val sc: StringContext) extends AnyVal {
    def ds(args: Recipe[_]*): Recipe[String] = new StringInterpolationRecipe(sc, args)

    // this builds the script as a String derivation first, and then runs it-- as opposed to the raw SystemDerivation where dependencies are passed as environment variables.
    def sys(args: Recipe[_]*): Recipe[ManagedPath] = new SystemRecipe(new StringInterpolationRecipe(sc, args), Map.empty)
  }

}

/*
class StringInterpolationSystemDerivation(sc: StringContext, args: Seq[Derivation[_]]) extends DerivableDerivation[Path] {

  private val resolvedProvenances = args.map(_.resolveOne)

  def derive: Provenance[TypedPath] with Successful[TypedPath] = {

    val startTime = DateTime.now()

    val result = {
      val resolvedArgs = resolvedProvenances.map(_.artifact.value)
      StringArtifact(sc.s(resolvedArgs))
    }
    val endTime = DateTime.now()

    SuccessfulProvenance(Identifier[Provenance[String]](UUID.randomUUID().toString), derivationId, ProvenanceStatus.Success, derivedFromUnnamed = resolvedProvenances.toSet, startTime = startTime, endTime = endTime, output = Some(result))
  }

  private val template: String = sc.parts.mkString(" ??? ")

  def derivationId = Identifier[Derivation[String]](WMHashHex(template + args.map(_.derivationId).mkString))

  def description = {
    "String Interpolation: " + template.take(80)
  }

  def dependencies = args.toSet
}

object StringInterpolationSystemDerivation {

  // Note: We extends AnyVal to prevent runtime instantiation.  See 
  // value class guide for more info.
  implicit class StringInterpolationDerivationHelper(val sc: StringContext) extends AnyVal {
    def sys(args: Derivation[_]*): Derivation[Path] = new StringInterpolationSystemDerivation(sc, args)
  }

}

*/
