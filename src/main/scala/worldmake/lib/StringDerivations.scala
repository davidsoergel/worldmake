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
import worldmake.derivationstrategy.FutureDerivationStrategy

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class StringInterpolationDerivation(sc: StringContext, args: Seq[Derivation[Any]]) extends DerivableDerivation[String] with Logging {

  def deriveFuture(implicit upstreamStrategy: FutureDerivationStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[String]](UUID.randomUUID().toString), derivationId)
    val resolvedProvenancesF: Future[Seq[Successful[Any]]] = Future.sequence(args.map(upstreamStrategy.resolveOne))
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
        logger.debug("Error in StringInterpolationDerivation: ", t) // todo better log message
        throw FailedDerivationException("Failed StringInterpolationDerivation", prf, t)
      }
    }
  }

  private val template: String = sc.parts.mkString("???").stripMargin.maskNewlines

  def derivationId = Identifier[Derivation[String]](WMHashHex(template + args.map(_.derivationId).mkString))

  def description = {
    val argIds = args.map(x => "${" + x.shortDesc + "}")

    // this stripMargin takes effect after interpolation; see https://github.com/scala/scala/pull/1655 for alternative
    val r = sc.s(argIds: _*).stripMargin
    "Interpolate: " + r.replace("\n", "\\n").limitAtWhitespace(80, "...")
  }

  def dependencies = args.toSet
}

object StringInterpolationDerivation {

  // Note: We extends AnyVal to prevent runtime instantiation.  See 
  // value class guide for more info.
  implicit class StringInterpolationDerivationHelper(val sc: StringContext) extends AnyVal {
    def ds(args: Derivation[_]*): Derivation[String] = new StringInterpolationDerivation(sc, args)

    // this builds the script as a String derivation first, and then runs it-- as opposed to the raw SystemDerivation where dependencies are passed as environment variables.
    def sys(args: Derivation[_]*): Derivation[Path] = new SystemDerivation(new StringInterpolationDerivation(sc, args), Map.empty)
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
