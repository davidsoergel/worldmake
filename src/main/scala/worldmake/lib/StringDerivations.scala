package worldmake.lib

import worldmake._
import org.joda.time.DateTime
import java.util.UUID
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import scala.Some
import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.StringUtils
import StringUtils._

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class StringInterpolationDerivation(sc: StringContext, args: Seq[Derivation[_]]) extends DerivableDerivation[String] {

  def derive: Provenance[String] with Successful[String] = {

    val resolvedProvenances = args.map(_.resolveOne)

    val startTime = DateTime.now()

    val result = {
      val resolvedArgs = resolvedProvenances.map(_.artifact.value)

      // this stripMargin taxes effect after interpolation; see https://github.com/scala/scala/pull/1655 for alternative
      StringArtifact(sc.s(resolvedArgs).stripMargin)
    }
    val endTime = DateTime.now()

    SuccessfulProvenance(Identifier[Provenance[String]](UUID.randomUUID().toString), derivationId, ProvenanceStatus.Success, derivedFromUnnamed = resolvedProvenances.toSet, startTime = startTime, endTime = endTime, output = Some(result))
  }

  private val template: String = sc.parts.mkString("???").maskNewlines

  def derivationId = Identifier[Derivation[String]](WMHashHex(template + args.map(_.derivationId).mkString))

  def description = {
    "String Interpolation: " + template.take(80)
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

  def derive: Provenance[Path] with Successful[Path] = {

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
