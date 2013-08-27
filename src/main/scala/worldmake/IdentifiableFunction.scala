package worldmake

import org.joda.time.DateTime
import worldmake.storage.Identifier
import java.util.UUID
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import scala.Some

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

class IdentifiableFunction0[R](val id: String, f: Function0[R]) {
  def apply() = f()
}


class Derivation0[R <: Hashable](f: IdentifiableFunction0[R]) extends DerivableDerivation[R] {
  def dependencies = Set.empty

  def derive: Provenance[R] with Successful[R] = {
    val startTime = DateTime.now()
    val result = new ContentHashableArtifact[R] {
      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f()
    }
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId,ProvenanceStatus.Success, startTime = startTime, endTime = endTime, output = Some(result))
  }

  def derivationId = Identifier[Derivation[R]](Hash.toHex(WMHash(f.id)))

  def description = f.id + "() "
}


class IdentifiableFunction1[T1, R](val id: String, f: Function1[T1, R]) {
  def apply(t1: T1) = f(t1)
}


class Derivation1[T1, R <: Hashable](f: IdentifiableFunction1[T1, R], a: Derivation[T1]) extends DerivableDerivation[R] {
  def derive: Provenance[R] with Successful[R] = {

    val startTime = DateTime.now()

    val p = a.resolveOne
    val result = new ContentHashableArtifact[R] {

      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f(p.artifact.value)
    }
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId, ProvenanceStatus.Success, derivedFromUnnamed = Set(p), startTime = startTime, endTime = endTime, output = Some(result))
  }

  def derivationId = Identifier[Derivation[R]](Hash.toHex(WMHash(f.id + a.derivationId)))

  def description = f.id

  def dependencies = Set(a)
}


