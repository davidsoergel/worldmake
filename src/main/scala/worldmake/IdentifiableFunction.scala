package worldmake

import java.util.UUID
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import scala.Some
import org.joda.time.DateTime
import java.io.InputStream
import worldmake.storage.Identifier
import scala.Some
import worldmake.storage.Identifier
import scala.Some

import scala.concurrent._

import ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import com.typesafe.scalalogging.slf4j.Logging

object NamedFunction {
  def apply[R](n:String)(f:Function0[R]) = new IdentifiableFunction0[R](n,f)
  def apply[T1,R](n:String)(f:Function1[T1,R]) = new IdentifiableFunction1[T1,R](n,f)
  def apply[T1,T2,R](n:String)(f:Function2[T1,T2,R]) = new IdentifiableFunction2[T1,T2,R](n,f)
}


trait Hashable {
  def contentHashBytes: Array[Byte]
  def contentHash = Hash.toHex(contentHashBytes)
}

/*
trait StreamHashable {
  /**
   * A canonical serialization of the entire artifact, or at least of sufficient identifying information to establish content equivalence.  Need not be sufficient to reconstruct the object.
   * @return
   */
  protected def bytesForContentHash: InputStream

  def contentHashBytes = WMHash(bytesForContentHash)
}
*/



trait ContentHashableArtifact[T] extends Artifact[T] {
  def contentHashBytes: Array[Byte] = value match {
  case h:Hashable =>h.contentHashBytes
  case i:Integer=>WMHash(i.toString)
  case d:Double=>WMHash(d.toString)
  case s:String=>WMHash(s)
  }
}


/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

class IdentifiableFunction0[R](val id: String, f: Function0[R]) {
  def evaluate() = f()
  def apply() = new Derivation0(this)
}


class Derivation0[R](f: IdentifiableFunction0[R]) extends DerivableDerivation[R] with Logging {
  def dependencies = Set.empty

  def deriveFuture(implicit strategy: FutureDerivationStrategy) = {
    val result = future { derive }
    result onFailure  {
      case t => {
        logger.debug("Error in Future: ", t)
      }
    }
    result
  }
  def derive: Provenance[R] with Successful[R] = {
    val startTime = DateTime.now()

    val result : Artifact[R] = Artifact[R](f.evaluate())
    /*
    val result = new ContentHashableArtifact[R] {
      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f.evaluate()
    }*/
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId,ProvenanceStatus.Success, startTime = startTime, endTime = endTime, output = Some(result))
  }

  def derivationId = Identifier[Derivation[R]](Hash.toHex(WMHash(f.id)))

  def description = f.id + "()"
}


class IdentifiableFunction1[T1, R](val id: String, f: Function1[T1, R]) {
  def evaluate(t1: T1) = f(t1)
  def apply(t1: Derivation[T1]) = new Derivation1(this,t1)
}


class Derivation1[T1, R](f: IdentifiableFunction1[T1, R], a: Derivation[T1]) extends DerivableDerivation[R] with Logging {
 
  
  private def deriveWithArg(p:Successful[T1]) : Provenance[R] with Successful[R] = {
    val startTime = DateTime.now()
    val result : Artifact[R] = Artifact[R](f.evaluate(p.artifact.value))
  
      /*new ContentHashableArtifact[R] {
      

      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f.evaluate(p.artifact.value)
    }*/
    
    
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId, ProvenanceStatus.Success, derivedFromUnnamed = Set(p), startTime = startTime, endTime = endTime, output = Some(result))
  }
  
 /* def derive: Provenance[R] with Successful[R] = {
    val p = a.resolveOne
    deriveWithArg(p)
  }*/

  def deriveFuture(implicit strategy: FutureDerivationStrategy): Future[Successful[R]] = {
    val pf = strategy.resolveOne(a)
    val result = pf.map(deriveWithArg)
    result onFailure  {
      case t => {
        logger.debug("Error in Future: ", t)
      }
    }
    result
  }

  def derivationId = Identifier[Derivation[R]](WMHashHex(f.id + a.derivationId))

  def description =  s"""${f.id}(${a.shortDesc})"""

  def dependencies = Set(a)

  // override def shortDesc =  s"""${f.id}(${a.shortDesc})"""
}


class IdentifiableFunction2[T1, T2, R](val id: String, f: Function2[T1, T2, R]) {
  def evaluate(t1: T1,t2: T2) = f(t1,t2)
  def apply(t1: Derivation[T1],t2: Derivation[T2]) = new Derivation2(this,t1,t2)
}


class Derivation2[T1,T2, R](f: IdentifiableFunction2[T1,T2, R], a: Derivation[T1],b:Derivation[T2]) extends DerivableDerivation[R] with Logging  {
  /*def derive: Provenance[R] with Successful[R] = {
    val p = a.resolveOne
    val q = b.resolveOne
    deriveWithArgs(p, q)
  }*/
  def deriveFuture(implicit strategy: FutureDerivationStrategy): Future[Provenance[R] with Successful[R]] = {
    val p = strategy.resolveOne(a)
    val q = strategy.resolveOne(b)
    val result = for {
      x <- p
      y <- q
    } yield deriveWithArgs(x, y)

    result onFailure  {
      case t => {
        logger.debug("Error in Future: ", t)
      }
    }
    result 
  }


  def deriveWithArgs(p: Successful[T1], q: Successful[T2]): Successful[R] = {
    val startTime = DateTime.now()

    val result : Artifact[R] = Artifact[R](f.evaluate(p.artifact.value, q.artifact.value))
    /*val result = new ContentHashableArtifact[R] {

      def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)

      def value = f.evaluate(p.artifact.value, q.artifact.value)
    }*/
    val endTime = DateTime.now()
    SuccessfulProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), derivationId, ProvenanceStatus.Success, derivedFromUnnamed = Set(p), startTime = startTime, endTime = endTime, output = Some(result))
  }

  def derivationId = Identifier[Derivation[R]](WMHashHex(f.id + a.derivationId + b.derivationId))

  def description = s"""${f.id}(${a.shortDesc},${b.shortDesc})"""

  def dependencies = Set(a,b)
}

