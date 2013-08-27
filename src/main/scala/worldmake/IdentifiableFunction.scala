package worldmake

import java.util.UUID
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import scala.Some
import org.joda.time.DateTime
import java.io.InputStream

object NamedFunction {
  def apply[R <: Hashable](n:String)(f:Function0[R]) = new IdentifiableFunction0[R](n,f)
  def apply[T1,R <: Hashable](n:String)(f:Function1[T1,R]) = new IdentifiableFunction1[T1,R](n,f)
  def apply[T1,T2,R <: Hashable](n:String)(f:Function2[T1,T2,R]) = new IdentifiableFunction2[T1,T2,R](n,f)
}


trait Hashable {
  /**
   * A canonical serialization of the entire artifact, or at least of sufficient identifying information to establish content equivalence.  Need not be sufficient to reconstruct the object.
   * @return
   */
  protected def bytesForContentHash: InputStream

  def contentHashBytes = WMHash(bytesForContentHash)
}

trait ContentHashableArtifact[T <: Hashable] extends Artifact[T] {
  def contentHashBytes: Array[Byte] = value.contentHashBytes
}


/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

class IdentifiableFunction0[R <: Hashable](val id: String, f: Function0[R]) {
  def evaluate() = f()
  def apply() = new Derivation0(this)
}


class Derivation0[R <: Hashable](f: IdentifiableFunction0[R]) extends DerivableDerivation[R] {
  def dependencies = Set.empty

  def derive: Provenance[R] with Successful[R] = {
    val startTime = DateTime.now()
    val result = new ContentHashableArtifact[R] {
      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f.evaluate()
    }
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId,ProvenanceStatus.Success, startTime = startTime, endTime = endTime, output = Some(result))
  }

  def derivationId = Identifier[Derivation[R]](Hash.toHex(WMHash(f.id)))

  def description = f.id + "() "
}


class IdentifiableFunction1[T1, R <: Hashable](val id: String, f: Function1[T1, R]) {
  def evaluate(t1: T1) = f(t1)
  def apply(t1: Derivation[T1]) = new Derivation1(this,t1)
}


class Derivation1[T1, R <: Hashable](f: IdentifiableFunction1[T1, R], a: Derivation[T1]) extends DerivableDerivation[R] {
  def derive: Provenance[R] with Successful[R] = {

    val startTime = DateTime.now()

    val p = a.resolveOne
    val result = new ContentHashableArtifact[R] {

      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f.evaluate(p.artifact.value)
    }
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId, ProvenanceStatus.Success, derivedFromUnnamed = Set(p), startTime = startTime, endTime = endTime, output = Some(result))
  }

  def derivationId = Identifier[Derivation[R]](WMHashHex(f.id + a.derivationId))

  def description = f.id

  def dependencies = Set(a)
}


class IdentifiableFunction2[T1, T2, R <: Hashable](val id: String, f: Function2[T1, T2, R]) {
  def evaluate(t1: T1,t2: T2) = f(t1,t2)
  def apply(t1: Derivation[T1],t2: Derivation[T2]) = new Derivation2(this,t1,t2)
}


class Derivation2[T1,T2, R <: Hashable](f: IdentifiableFunction2[T1,T2, R], a: Derivation[T1],b:Derivation[T2]) extends DerivableDerivation[R] {
  def derive: Provenance[R] with Successful[R] = {

    val startTime = DateTime.now()

    val p = a.resolveOne
    val q = b.resolveOne
    val result = new ContentHashableArtifact[R] {

      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f.evaluate(p.artifact.value,q.artifact.value)
    }
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId, ProvenanceStatus.Success, derivedFromUnnamed = Set(p), startTime = startTime, endTime = endTime, output = Some(result))
  }

  def derivationId = Identifier[Derivation[R]](WMHashHex(f.id + a.derivationId + b.derivationId))

  def description = f.id

  def dependencies = Set(a)
}

