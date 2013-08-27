package worldmake.lib

import worldmake._
import org.joda.time.DateTime
import java.util.UUID
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import scala.Some

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class StringInterpolationDerivation(sc: StringContext, args: Seq[Derivation[_]]) extends DerivableDerivation[String] {

  private val resolvedProvenances = args.map(_.resolveOne)

  def derive: Provenance[String] with Successful[String] = {

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

object StringInterpolationDerivation {

  // Note: We extends AnyVal to prevent runtime instantiation.  See 
  // value class guide for more info.
  implicit class StringInterpolationDerivationHelper(val sc: StringContext) extends AnyVal {
    def ds(args: Derivation[_]*): Derivation[String] = new StringInterpolationDerivation(sc, args)
  }

}
