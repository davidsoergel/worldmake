
package worldmake

import java.util.UUID
import worldmake.storage.{ExternalPathArtifact, ManagedPathArtifact, Identifier}
import scala.collection.{GenIterable, GenTraversable, GenSet}
import scala.collection.immutable.Queue
import scala.concurrent._
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.cookingstrategy.CookingStrategy
import scala.Some
import org.joda.time.DateTime
import scalax.file.Path
import scala.util.{Failure, Success}

//import java.lang.ProcessBuilder.Redirect

import worldmake.WorldMakeConfig._

//import scala.collection.JavaConversions._


import edu.umass.cs.iesl.scalacommons.StringUtils._
import ExecutionContext.Implicits.global

// even constant artifacts must be stored in the DB, to provide provenance even when the code changes, etc.

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Recipe[+T] extends WorldmakeEntity {
  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  // careful using a hash as an ID, per Valerie Aurora
  def recipeId: Identifier[Recipe[T]]

  lazy val description: String = longDescription.firstLine.limitAtWhitespace(80, "...") // used only for human-readable debug logs and such

  def longDescription: String

  private var providedSummary: String = ""

  def setProvidedSummary(s: String) {
    providedSummary = s
  }

  def summary = providedSummary

  lazy val shortId = recipeId.short

  lazy val shortDesc = shortId

  lazy val queue: Queue[Recipe[_]] = Queue(this)

  lazy val isGloballyDeterministic: Boolean = true

  override def equals(other: Any): Boolean = other match {
    case that: Recipe[T] => (that canEqual this) && recipeId == that.recipeId
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Recipe[T]]

  override def hashCode: Int = (41 + recipeId.hashCode)

  //def stage(implicit upstreamStrategy: CookingStrategy): Provenance[T]

  def deriveFuture(implicit upstreamStrategy: CookingStrategy): Future[Successful[T]]

}

object Recipe {
  implicit def toDescribedRecipe(s: String): RecipeSummary = new RecipeSummary(s)
}

class RecipeSummary(s: String) {
  def via[T <: Recipe[_]](d: T): T = {
    d.setProvidedSummary(s) //copy(description = s)
    d
  }
}

/*
object DerivationStatuses {
sealed abstract class DerivationStatus(val name: String)

case object Constant extends DerivationStatus("Constant")

case object Cached extends DerivationStatus("Cached")

case object Blocked extends DerivationStatus("Blocked")

case object Pending extends DerivationStatus("Pending")

case object Error extends DerivationStatus("Error")
}*/


object ConstantRecipe {
  def apply[T](p: ConstantProvenance[T]) = new ConstantRecipe(p)

  implicit def fromString(s: String): ConstantRecipe[String] = ConstantRecipe(ConstantProvenance(StringArtifact(s)))

  implicit def fromBoolean(s: Boolean): ConstantRecipe[Boolean] = ConstantRecipe(ConstantProvenance(BooleanArtifact(s)))

  implicit def fromDouble(s: Double): ConstantRecipe[Double] = ConstantRecipe(ConstantProvenance(DoubleArtifact(s)))

  implicit def fromInt(s: Int): ConstantRecipe[Int] = ConstantRecipe(ConstantProvenance(IntArtifact(s)))

  // don't allow this: use RecipeWrapper.pathFromString instead.  Otherwise you end up rehashing the file all the time.
  //implicit def fromPath(s: Path): ConstantRecipe[ManagedPath] = ConstantRecipe(ConstantProvenance(PathArtifact(s)))

  //  this would be OK, but it would be better to force use of RecipeWrapper directly for clarity
  // implicit def pathFromString(s: String): Recipe[ManagedPath] = RecipeWrapper.pathFromString(s)
}

class ConstantRecipe[T](p: ConstantProvenance[T]) extends Recipe[T] with (() => ConstantProvenance[T]) {
  lazy val recipeId = Identifier[Recipe[T]](p.provenanceId.s)

  private lazy val outputString: String = p.output.value.toString.replace("\n", "\\n")

  lazy val longDescription = outputString

  def apply = p

  def deriveFuture(implicit upstreamStrategy: CookingStrategy) = Future.successful(p)

  /*future {
    p
  } // (promise[Successful[T]]() success p).future 
  */

  //  def statusString: String = ProvenanceStatus.Constant.toString

  override lazy val shortId = "         "

  override lazy val shortDesc = if (outputString.matches("[\\s]")) outputString.limitAtWhitespace(30, "...") else outputString
}


trait DerivableRecipe[T] extends Recipe[T] {

  //def dependencies : A
  def dependencies: GenSet[Recipe[_]]

  override lazy val queue: Queue[Recipe[_]] = {
    val deps = dependencies.seq.toSeq.flatMap(_.queue)
    Queue[Recipe[_]](deps: _*).distinct.enqueue(this)
  }

}


trait LocallyDeterministic[T] extends DerivableRecipe[T] {
  override lazy val isGloballyDeterministic: Boolean = !dependencies.exists(!_.isGloballyDeterministic)
}

trait LocallyNondeterministic[T] extends DerivableRecipe[T] {
  def resolvePrecomputed: GenSet[Provenance[T]]

  //def resolveNew: Option[Provenance[T]]

  override lazy val isGloballyDeterministic = false
}


/*
class SystemDerivationJava(val script: Derivation[String], namedDependencies: Map[String, Derivation[_]]) extends ExternalPathDerivation with DerivableDerivation[Path] with Logging {

  def dependencies = namedDependencies.values.toSet

  // "toEnvironmentString" is not a method of the Derivation trait because the Any->String conversion may differ by 
  // context (at least, eg., # of sig figs, or filename vs file contents, etc.)
  // For that matter, what if it differs for different arguments of the same type? 
  def toEnvironmentString[T](x:Artifact[T]) : String = x match {
    case f:ExternalPathArtifact => f.abspath
    case f:TraversableArtifact => f.artifacts.map(toEnvironmentString).mkString(" ")
    //case f:GenTraversableArtifact => f.artifacts.map(toEnvironmentString).mkString(" ")
    case f => f.value.toString
  }

  def derive = {
    val outputPath: Path = fileStore.newPath
    val reifiedDependencies = namedDependencies.mapValues(_.resolve)
    // environment is only mutable for the sake of Java conversion
    val environment : mutable.Map[String,String] = mutable.Map[String,String]().addAll(reifiedDependencies.mapValues(toEnvironmentString))
    val workingDir = Path.createTempDirectory()

    val pb = new ProcessBuilder("/bin/sh", "./worldmake.runner")
    pb.directory(workingDir.jfile) //fileOption.getOrElse(throw new Error("bad temp dir: " + workingDir)))

    val pbenv = pb.environment()
    pbenv.putAll(WorldMakeConfig.globalEnvironment)
    pbenv.putAll(environment)
    pbenv.put("out", workingDir.toAbsolute.path)
    pbenv.put("PATH", aoeaoe)

    val log: File = (outputPath / "worldmake.log").fileOption.getOrElse(throw new Error("can't create log: " + outputPath / "worldmake.log"))
    pb.redirectErrorStream(true)
    pb.redirectOutput(Redirect.appendTo(log))
    val p: Process = pb.start()

    assert(pb.redirectInput() == Redirect.PIPE)
    assert(pb.redirectOutput().file() == log)
    assert(p.getInputStream.read() == -1)

    val exitCode = p.waitFor()

    val result = new ExternalPathArtifact(outputPath) with DerivedArtifact[TypedPath] {
      def derivedFrom = SystemDerivation.this
    }


    if (exitCode != 0) {
      logger.warn("Retaining working directory: " + workingDir)
      throw new FailedDerivationException
    }

    if (WorldMakeConfig.debugWorkingDirectories) {
      logger.warn("Retaining working directory: " + workingDir)
    } else {
      workingDir.deleteRecursively()
    }
    result
  }
}
*/

//http://mblinn.com/blog/2012/06/30/scala-custom-exceptions/
object FailedRecipeException {
  def apply(message: String, recipeId: Identifier[Recipe[_]]): FailedRecipeException = new FailedRecipeException(message, recipeId, None)

  def apply(message: String, pr: FailedProvenance[_]): FailedRecipeException = new FailedRecipeException(message, pr.recipeId, Some(pr))

  def apply(message: String, pr: FailedProvenance[_], cause: Throwable) = new FailedRecipeException(message, pr.recipeId, Some(pr)).initCause(cause)
}

class FailedRecipeException(message: String, recipeId: Identifier[Recipe[_]], opr: Option[FailedProvenance[_]]) extends Exception(message)

/*
trait ExternalPathDerivation extends Derivation[Path] {

  //def children : Seq[ExternalPathArtifact]
  def /(s: String): Derivation[Path] = new Derivation1[Path, Path](new IdentifiableFunction1[Path, Path]("/", (p: Path) => p / s), this) //with ExternalPathDerivation

}
*/

/*
class TraversableProvenance[T](val provenances: Traversable[Provenance[T]]) extends Provenance[Traversable[T]] {
  def provenanceId = Identifier[Provenance[Traversable[T]]](UUID.randomUUID().toString)

  def output = Some(new TraversableArtifact[T](provenances.map))

  def status = ???
}
*/
object TraversableRecipe extends Logging {
  implicit def wrapTraversable[T](xs: GenTraversable[Recipe[T]]) = new TraversableRecipe(xs)

  //implicit def unwrapTraversable[T](t:Recipe[GenTraversable[Artifact[T]]]) : GenTraversable[Recipe[T]] = 


  // how to unwrap  
  def seqGet[T] = new IdentifiableFunction2[Seq[T], Int, T]("seqGet", {
    (a: Seq[T], b: Int) => {
      logger.debug(s"seqGet getting element $b of a Seq of size ${a.size}")
      //logger.debug(a.toString())
      a(b)
    }
  })

  /*
  def seqRecipeUnwrap[T](r: Recipe[GenTraversable[T]])(implicit upstreamStrategy: CookingStrategy): Iterator[Recipe[T]] = {
    val f = r.deriveFuture.map(x => {
      // get the completed sequence
      val s = x.output.value

      // make a provenance for each element
      val pp: GenIterable[CompletedProvenance[T]] = s.toSeq.zipWithIndex.map({
        case (elem, index) => {
          InstantCompletedProvenance(Identifier[Provenance[T]](UUID.randomUUID().toString), Identifier[Recipe[T]](r.recipeId.s + "/" + index), Set.empty, Map.empty, x.createdTime, x.enqueuedTime, x.startTime, x.runningInfo, x.endTime, x.exitCode, None, Map.empty, Artifact(elem))
        }})

        pp
      })

    // we don't know how many elements the sequence should have until we resolve the Future!
    // so we have to return an Iterator, which just resolves the underlying Seq on the first call.
   
    
    val result:  GenIterable[Recipe[T]] = new Iterator[Recipe[T]]{}
      
      
      f onComplete {
      case Success(x) => {
        val w = x.zipWithIndex.map({case (p: CompletedProvenance[T], index) => new Recipe[T]{
          def deriveFuture(implicit upstreamStrategy: CookingStrategy) = Future.successful(p)
          def longDescription = r.longDescription + "/" + index
          def recipeId = p.recipeId
        }})
      w
      }
      case Failure(t) => {
        logger.error("Failed to unwrap traversable recipes.", t)
        throw t
      }
    }



result

  }*/

  }

  class TraversableRecipe[T](val xs: GenTraversable[Recipe[T]]) extends DerivableRecipe[GenTraversable[Artifact[T]]] with Logging {
    /*def derive = {
      val upstream = xs.par.map(_.resolveOne)
      SuccessfulProvenance[GenTraversable[T]](Identifier[Provenance[GenTraversable[T]]](UUID.randomUUID().toString),
        derivationId, ProvenanceStatus.Success,
        derivedFromUnnamed = upstream.toSet.seq,
        output = Some(new GenTraversableArtifact(upstream.map(_.artifact))))
    }
  
    def deriveFuture = {
      val upstreamFF = xs.map(_.resolveOneFuture)
      val upstreamF = Future.sequence(upstreamFF.seq)
      upstreamF.map(upstream => {
        SuccessfulProvenance[GenTraversable[T]](Identifier[Provenance[GenTraversable[T]]](UUID.randomUUID().toString),
          derivationId, ProvenanceStatus.Success,
          derivedFromUnnamed = upstream.toSet.seq,
          output = Some(new GenTraversableArtifact(upstream.map(_.artifact))))
      })
    }*/

    /*
     def deriveWithArgs(derivedFromUnnamed:GenTraversable[Successful[_]]) = {
      SuccessfulProvenance[GenTraversable[T]](Identifier[Provenance[GenTraversable[T]]](UUID.randomUUID().toString),
        derivationId, ProvenanceStatus.Success,
        derivedFromUnnamed = derivedFromUnnamed.toSet.seq,
        output = Some(new GenTraversableArtifact(derivedFromUnnamed.map(_.artifact))))
    }*/

    // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
    lazy val recipeId = Identifier[TraversableRecipe[T]](WMHashHex("traversable" + xs.toSeq.par.map(_.recipeId).mkString))

    lazy val longDescription = ("Traversable(" + xs.map(_.description) + ")") //.limitAtWhitespace(80, "...")

    lazy val dependencies: GenSet[Recipe[_]] = xs.toSet

    /*  def apply(args: ArgumentsSet[AA]) = {
        val argValues = args.resolved.values
        SuccessfulProvenance[GenTraversable[AA]](Identifier[Provenance[GenTraversable[AA]]](UUID.randomUUID().toString),
          derivationId, ProvenanceStatus.Success,
          derivedFromUnnamed = argValues, //derivedFromUnnamed.toSet.seq,
          output = Some(new GenTraversableArtifact(argValues.map(_.artifact))))
      }
    */
    /*def stage(implicit upstreamStrategy: CookingStrategy) = {
  
      val pr = BlockedProvenance(Identifier[Provenance[GenTraversable[Artifact[T]]]](UUID.randomUUID().toString), recipeId)
      pr
    }*/

    def deriveFuture(implicit upstreamStrategy: CookingStrategy) = {

      val pr = BlockedProvenance(Identifier[Provenance[GenTraversable[Artifact[T]]]](UUID.randomUUID().toString), recipeId)
      val upstreamFF = xs.map(upstreamStrategy.cookOne)
      val upstreamF = Future.sequence(upstreamFF.seq)
      val result: Future[CompletedProvenance[GenTraversable[Artifact[T]]]] = upstreamF.map(upstream => deriveWithArg(pr.pending(upstream.toSet, Map.empty), upstream))
      result
    }


    private def deriveWithArg(pr: PendingProvenance[GenTraversable[Artifact[T]]], a1: Traversable[Successful[T]]): CompletedProvenance[GenTraversable[Artifact[T]]] = {
      val prs = pr.running(new MemoryWithinJvmRunningInfo)
      try {
        val artifact: GenTraversableArtifact[T] = new MemoryGenTraversableArtifact(a1.map(_.output)) //Artifact[GenTraversable[T]](f.evaluate(a1.output.value))
        val result: CompletedProvenance[GenTraversable[Artifact[T]]] = prs.completed(0, None, Map.empty, artifact)
        result
      }
      catch {
        case t: Throwable => {
          val prf = prs.failed(1, None, Map.empty)
          logger.debug("Error in TraversableRecipe: ", t) // todo better log message
          throw FailedRecipeException("Failed TraversableRecipe", prf, t)
        }
      }
    }


  }

  /*
  class DerivationSet(xs: Traversable[Derivation[_]]) extends DerivableDerivation[Traversable[_]] {
    def derive = {
      val upstream = xs.map(_.resolveOne)
      SuccessfulProvenance[Traversable[_]](Identifier[Provenance[Traversable[_]]](UUID.randomUUID().toString),
        derivationId, ProvenanceStatus.Success,
        derivedFromUnnamed = upstream.toSet,
        output = Some(new TraversableArtifact(upstream.map(_.artifact))))
    }
  
    // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
    def derivationId = Identifier[Derivation[Traversable[_]]](WMHashHex("set" + xs.toSeq.map(_.derivationId).mkString))
  
    def description = ("Traversable(" + xs.map(_.description) + ")").take(40)
  
    def dependencies = xs.toSet
  }
  */

  /*
  class GenTraversableArtifact[T](val artifacts: GenTraversable[Artifact[T]])  extends Artifact[GenTraversable[T]] {
    def contentHashBytes = artifacts.seq.toSeq.sorted.map(_.contentHashBytes).flatten.toArray
    lazy val value = artifacts.map(_.value)
  }
  
  class GenTraversableDerivation[T](xs: GenTraversable[Derivation[T]]) extends Derivation[GenTraversable[T]]{
    def resolve = new GenTraversableArtifact(xs.map(_.resolve))
  
    // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
    def uniqueId = Hash.toHex(Hash("SHA-256",xs.seq.toSeq.sorted.map(_.uniqueId).mkString))
  }
  */

  object ManagedPathRecipe {
    implicit def toManagedPathRecipe(r: Recipe[ManagedPath]) = new ManagedPathRecipe(r)
  }

  class ManagedPathRecipe(underlying: Recipe[ManagedPath]) extends Recipe[ManagedPath] {
    def deriveFuture(implicit upstreamStrategy: CookingStrategy) = underlying.deriveFuture

    def longDescription = underlying.longDescription

    def recipeId = underlying.recipeId

    def /(s: String): ManagedPathRecipe = new ManagedPathRecipe(new DerivableRecipe[ManagedPath] {
      //def deriveFuture(implicit upstreamStrategy: CookingStrategy) = ManagedPathRecipe.this.deriveFuture.map(s=>new MemoryCompletedProvenance[ManagedPath]())

      def dependencies = Set(underlying)

      def deriveFuture(implicit upstreamStrategy: CookingStrategy): Future[Successful[ManagedPath]] = {
        //val pr = BlockedProvenance(Identifier[Provenance[ManagedPath]](UUID.randomUUID().toString), recipeId)
        val pf = upstreamStrategy.cookOne(ManagedPathRecipe.this)
        val now = new DateTime()
        val result: Future[Successful[ManagedPath]] = pf.map((a1: Successful[ManagedPath]) =>
          InstantCompletedProvenance[ManagedPath](
            Identifier[Provenance[ManagedPath]](UUID.randomUUID().toString),
            recipeId,
            Set(a1),
            Map.empty,
            now,
            now,
            now,
            new MemoryWithinJvmRunningInfo,
            now,
            0,
            None,
            Map.empty,
            ManagedPathArtifact(ManagedPath(a1.output.value.id, a1.output.value.relative / s))))
        result
      }

      def longDescription = ManagedPathRecipe.this.longDescription + "/" + s

      // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
      def recipeId = Identifier[Recipe[ManagedPath]](WMHashHex(ManagedPathRecipe.this.recipeId.s + "/" + s))
    })


    def children(): Recipe[GenTraversable[Artifact[ManagedPath]]] = new DerivableRecipe[GenTraversable[Artifact[ManagedPath]]] {

      def dependencies = Set(underlying)

      def deriveFuture(implicit upstreamStrategy: CookingStrategy): Future[Successful[GenTraversable[Artifact[ManagedPath]]]] = ManagedPathRecipe.this.deriveFuture.map((r: Successful[ManagedPath]) => {
        val m = r.output.value
        val cpaths = m.path.children().toSet.map((c: Path) => ManagedPath(m.id, m.relative / c.path))
        val now = new DateTime()
        val cpathartifacts: GenTraversable[Artifact[ManagedPath]] = cpaths.map(ManagedPathArtifact(_))

        InstantCompletedProvenance[GenTraversable[Artifact[ManagedPath]]](
          Identifier[Provenance[GenTraversable[Artifact[ManagedPath]]]](UUID.randomUUID().toString),
          recipeId,
          Set(r),
          Map.empty,
          now,
          now,
          now,
          new MemoryWithinJvmRunningInfo,
          now,
          0,
          None,
          Map.empty,
          new MemoryGenTraversableArtifact[ManagedPath](cpathartifacts))


      })

      def longDescription = ManagedPathRecipe.this.longDescription + "/*"

      // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
      def recipeId = Identifier[Recipe[GenTraversable[Artifact[ManagedPath]]]](WMHashHex(ManagedPathRecipe.this.recipeId.s + "/*"))
    }


    //}
  }


  object ExternalPathRecipe {
    implicit def toExternalPathRecipe(r: Recipe[ExternalPath]) = new ExternalPathRecipe(r)
  }

  class ExternalPathRecipe(underlying: Recipe[ExternalPath]) extends Recipe[ExternalPath] with Logging {
    def deriveFuture(implicit upstreamStrategy: CookingStrategy) = underlying.deriveFuture

    def longDescription = underlying.longDescription

    def recipeId = underlying.recipeId

    def /(s: String): ExternalPathRecipe = new ExternalPathRecipe(new DerivableRecipe[ExternalPath] {
      //def deriveFuture(implicit upstreamStrategy: CookingStrategy) = ExternalPathRecipe.this.deriveFuture.map(s=>new MemoryCompletedProvenance[ExternalPath]())

      def dependencies = Set(underlying)

      def deriveFuture(implicit upstreamStrategy: CookingStrategy): Future[Successful[ExternalPath]] = ExternalPathRecipe.this.deriveFuture.map((r: Successful[ExternalPath]) => {

        val now = new DateTime()

        try {
          val now = new DateTime()

          InstantCompletedProvenance[ExternalPath](
            Identifier[Provenance[ExternalPath]](UUID.randomUUID().toString),
            recipeId,
            Set(r),
            Map.empty,
            now,
            now,
            now,
            new MemoryWithinJvmRunningInfo,
            now,
            0,
            None,
            Map.empty,
            ExternalPathArtifact(ExternalPath(r.output.value.path / s)))

        } catch {
          case t: Throwable => {
            val prf = InstantFailedProvenance[ExternalPath](
              Identifier[Provenance[ExternalPath]](UUID.randomUUID().toString),
              recipeId,
              Set(r),
              Map.empty,
              now,
              now,
              now,
              new MemoryWithinJvmRunningInfo,
              now,
              0,
              None,
              Map.empty)
            logger.debug("Error in ExternalPathRecipe./: ", t) // todo better log message
            throw FailedRecipeException("Failed ExternalPathRecipe./", prf, t)
          }
        }
      })

      def longDescription = ExternalPathRecipe.this.longDescription + "/" + s

      // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
      def recipeId = Identifier[Recipe[ExternalPath]](WMHashHex(ExternalPathRecipe.this.recipeId.s + "/" + s))
    })


    def children(): Recipe[GenTraversable[Artifact[ExternalPath]]] = new DerivableRecipe[GenTraversable[Artifact[ExternalPath]]] {

      def dependencies = Set(underlying)

      def deriveFuture(implicit upstreamStrategy: CookingStrategy): Future[Successful[GenTraversable[Artifact[ExternalPath]]]] = ExternalPathRecipe.this.deriveFuture.map((r: Successful[ExternalPath]) => {

        val now = new DateTime()

        try {
          val m = r.output.value

          val cpaths = {
            val ch: Set[Path] = m.path.children().toSet
            val exch = ch.map((c: Path) => ExternalPath(c))
            exch
          }

          val cpathartifacts: GenTraversable[Artifact[ExternalPath]] = cpaths.map(ExternalPathArtifact(_))
 
          logger.debug(s"Found ${cpathartifacts.size} children of ${m.path}.")
          InstantCompletedProvenance[GenTraversable[Artifact[ExternalPath]]](
            Identifier[Provenance[GenTraversable[Artifact[ExternalPath]]]](UUID.randomUUID().toString),
            recipeId,
            Set(r),
            Map.empty,
            now,
            now,
            now,
            new MemoryWithinJvmRunningInfo,
            now,
            0,
            None,
            Map.empty,
            new MemoryGenTraversableArtifact[ExternalPath](cpathartifacts))


        }
        catch {
          case t: Throwable => {
            val prf = InstantFailedProvenance[GenTraversable[Artifact[ExternalPath]]](
              Identifier[Provenance[GenTraversable[Artifact[ExternalPath]]]](UUID.randomUUID().toString),
              recipeId,
              Set(r),
              Map.empty,
              now,
              now,
              now,
              new MemoryWithinJvmRunningInfo,
              now,
              0,
              None,
              Map.empty)
            logger.debug("Error in ExternalPathRecipe.children: ", t) // todo better log message
            throw FailedRecipeException("Failed ExternalPathRecipe.children", prf, t)
          }
        }
      })

      def longDescription = ExternalPathRecipe.this.longDescription + "/*"

      // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
      def recipeId = Identifier[Recipe[GenTraversable[Artifact[ExternalPath]]]](WMHashHex(ExternalPathRecipe.this.recipeId.s + "/*"))
    }


    //}
  }
