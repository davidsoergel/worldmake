package worldmake

import java.util.UUID
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier

import scala.concurrent._

import ExecutionContext.Implicits.global
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.cookingstrategy.CookingStrategy
import scala.collection.GenSet

object NamedFunction {
  def apply[R](n: String)(f: Function0[R]) = new IdentifiableFunction0[R](n, f)

  def apply[T1, R](n: String)(f: Function1[T1, R]) = new IdentifiableFunction1[T1, R](n, f)

  def apply[T1, T2, R](n: String)(f: Function2[T1, T2, R]) = new IdentifiableFunction2[T1, T2, R](n, f)
}


trait Hashable {
  def contentHashBytes: Array[Byte]

  lazy val contentHash = Hash.toHex(contentHashBytes)
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
  lazy val contentHashBytes: Array[Byte] = value match {
    case h: Hashable => h.contentHashBytes
    case i: Int => WMHash(i.toString)
    case d: Double => WMHash(d.toString)
    case d: Boolean => WMHash(d.toString)
    case s: String => WMHash(s)
  }
}


/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

class IdentifiableFunction0[R](val id: String, f: Function0[R]) {
  def evaluate() = f()

  def apply() = new Recipe0(this)
}


class Recipe0[R](f: IdentifiableFunction0[R]) extends DerivableRecipe[R] with Logging {
  lazy val dependencies : GenSet[Recipe[_]]= Set.empty

  def deriveFuture(implicit upstreamStrategy: CookingStrategy) = {
    val pr = BlockedProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), recipeId)
    val result = future {
      derive(pr.pending(Set.empty,Map.empty))
    }
    result
  }

  private def derive(pr: PendingProvenance[R]): CompletedProvenance[R] = {
    val prs = pr.running(new MemoryWithinJvmRunningInfo)
    try {
      val result: Artifact[R] = Artifact[R](f.evaluate())
      /*
      val result = new ContentHashableArtifact[R] {
        def artifactId = Identifier[Artifact[R]](UUID.randomUUID().toString)
  
        def value = f.evaluate()
      }*/
      //val endTime = DateTime.now()
      prs.completed(0, None, Map.empty, result)
      //SuccessfulProvenance(pr.provenanceId, derivationId,ProvenanceStatus.Success, startTime = Some(startTime), endTime = Some(endTime), output = Some(result))
    }
    catch {
      case t: Throwable => {
        val prf = prs.failed(1, None, Map.empty)
        logger.debug("Error in Recipe0: ", t) // todo better log message
        throw FailedRecipeException("Failed Recipe0", prf, t)
      }
    }
  }

  lazy val  recipeId = Identifier[Recipe[R]](Hash.toHex(WMHash(f.id)))

  lazy val  longDescription = f.id + "()"
}


class IdentifiableFunction1[T1, R](val id: String, f: Function1[T1, R]) {
  def evaluate(t1: T1) = f(t1)

  def apply(t1: Recipe[T1]) = new Recipe1(this, t1)
}


class Recipe1[T1, R](f: IdentifiableFunction1[T1, R], a: Recipe[T1]) extends DerivableRecipe[R] with Logging {

  def deriveFuture(implicit upstreamStrategy: CookingStrategy): Future[Successful[R]] = {
    val pr = BlockedProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), recipeId)
    val pf = upstreamStrategy.cookOne(a)
    val result = pf.map(a1 => deriveWithArg(pr.pending(Set(a1),Map.empty), a1))
    result
  }

  private def deriveWithArg(pr: PendingProvenance[R], a1: Successful[T1]): CompletedProvenance[R] = {
    val prs = pr.running(new MemoryWithinJvmRunningInfo)
    try {
      val result: Artifact[R] = Artifact[R](f.evaluate(a1.output.value))
      prs.completed(0, None, Map.empty, result)
    }
    catch {
      case t: Throwable => {
        val prf = prs.failed(1, None, Map.empty)
        logger.debug("Error in Recipe1: ", t) // todo better log message
        throw FailedRecipeException("Failed Recipe1", prf, t)
      }
    }
  }

  lazy val  recipeId = Identifier[Recipe[R]](WMHashHex(f.id + a.recipeId))

  lazy val  longDescription = s"""${f.id}(${a.shortDesc})"""

  lazy val  dependencies : GenSet[Recipe[_]] = Set(a)

  // override def shortDesc =  s"""${f.id}(${a.shortDesc})"""
}


class IdentifiableFunction2[T1, T2, R](val id: String, f: Function2[T1, T2, R]) {
  def evaluate(t1: T1, t2: T2) = f(t1, t2)

  def apply(t1: Recipe[T1], t2: Recipe[T2]) = new Recipe2(this, t1, t2)
}


class Recipe2[T1, T2, R](f: IdentifiableFunction2[T1, T2, R], a: Recipe[T1], b: Recipe[T2]) extends DerivableRecipe[R] with Logging {

  def deriveFuture(implicit upstreamStrategy: CookingStrategy): Future[Provenance[R] with Successful[R]] = {

    val pr = BlockedProvenance(Identifier[Provenance[R]](UUID.randomUUID().toString), recipeId)
    val p = upstreamStrategy.cookOne(a)
    val q = upstreamStrategy.cookOne(b)
    val result = for {
      x <- p
      y <- q
    } yield deriveWithArgs(pr.pending(Set(x,y),Map.empty), x, y)
    result
  }


  def deriveWithArgs(pr: PendingProvenance[R], p: Successful[T1], q: Successful[T2]): Successful[R] = {
    val prs = pr.running(new MemoryWithinJvmRunningInfo)
    try {
      val result: Artifact[R] = Artifact[R](f.evaluate(p.output.value, q.output.value))
      prs.completed(0, None, Map.empty, result)
    }
    catch {
      case t: Throwable => {
        val prf = prs.failed(1, None, Map.empty)
        logger.debug("Error in Recipe2: ", t) // todo better log message
        throw FailedRecipeException("Failed Recipe2", prf, t)
      }
    }
  }

  lazy val  recipeId = Identifier[Recipe[R]](WMHashHex(f.id + a.recipeId + b.recipeId))

  lazy val  longDescription = s"""${f.id}(${a.shortDesc},${b.shortDesc})"""

  lazy val  dependencies = Set(a, b)
}

