
package worldmake

import java.util.UUID
import worldmake.storage.{Storage, Identifier}
import scala.collection.{GenTraversable, GenSet}
import scala.collection.immutable.Queue
import scala.concurrent._

//import java.lang.ProcessBuilder.Redirect

import WorldMakeConfig._

//import scala.collection.JavaConversions._

import scalax.file.Path

import edu.umass.cs.iesl.scalacommons.StringUtils._
import ExecutionContext.Implicits.global

// even constant artifacts must be stored in the DB, to provide provenance even when the code changes, etc.

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Derivation[+T] {
  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  // careful using a hash as an ID, per Valerie Aurora
  def derivationId: Identifier[Derivation[T]]

  def description: String // used only for human-readable debug logs and such

  def shortId = derivationId.short

  def shortDesc = shortId

  def queue: Queue[Derivation[_]] = Queue(this)

  def isGloballyDeterministic: Boolean = true

  override def equals(other: Any): Boolean = other match {
    case that: Derivation[T] => (that canEqual this) && derivationId == that.derivationId
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Derivation[T]]

  override def hashCode: Int = (41 + derivationId.hashCode)


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


object ConstantDerivation {
  def apply[T](p: ConstantProvenance[T]) = new ConstantDerivation(p)

  implicit def fromString(s: String): ConstantDerivation[String] = ConstantDerivation(ConstantProvenance(StringArtifact(s)))

  implicit def fromDouble(s: Double): ConstantDerivation[Double] = ConstantDerivation(ConstantProvenance(DoubleArtifact(s)))

  implicit def fromInteger(s: Integer): ConstantDerivation[Integer] = ConstantDerivation(ConstantProvenance(IntegerArtifact(s)))

  implicit def fromInt(s: Int): ConstantDerivation[Integer] = ConstantDerivation(ConstantProvenance(IntegerArtifact(s)))

  implicit def fromPath(s: Path): ConstantDerivation[Path] = ConstantDerivation(ConstantProvenance(ExternalPathArtifact(s)))
}

class ConstantDerivation[T](p: ConstantProvenance[T]) extends Derivation[T] with (()=>ConstantProvenance[T]) {
  def derivationId = Identifier[Derivation[T]](p.provenanceId.s)

  private val value: String = p.output.get.value.toString.replace("\n", "\\n")

  def description = {
    //"Input: " + 
    value.limitAtWhitespace(80, "...")
  }

  def apply = p
/*
  def resolveOneFuture = future {
    p
  } // (promise[Successful[T]]() success p).future 

  def statusString: String = ProvenanceStatus.Constant.toString
*/
  override def shortId = "         "

  override def shortDesc = if (value.matches("[\\s]")) value.limitAtWhitespace(30, "...") else value
}


trait DerivableDerivation[A<:UnresolvedArguments,T] extends Derivation[T] with ((ResolvedArguments)=>Successful[T]){

  //def dependencies : A
  def dependencies : A //= args.unresolved //: A //GenSet[Derivation[_]]

  override def queue: Queue[Derivation[_]] = {
    val deps = dependencies.queue //seq.toSeq.flatMap(_.getQueue)
    Queue[Derivation[_]](deps: _*).distinct.enqueue(this)
  }
}


trait LocallyDeterministic[A<:UnresolvedArguments,T] extends DerivableDerivation[A,T] {
  override def isGloballyDeterministic: Boolean = !dependencies.queue.exists(!_.isGloballyDeterministic)
}

trait LocallyNondeterministic[A<:UnresolvedArguments,T] extends DerivableDerivation[A,T] {
  def resolvePrecomputed: GenSet[Provenance[T]]

  //def resolveNew: Option[Provenance[T]]

  override def isGloballyDeterministic = false
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

    val result = new ExternalPathArtifact(outputPath) with DerivedArtifact[Path] {
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


class FailedDerivationException extends Exception

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

class TraversableDerivation[AA](xs: GenTraversable[Derivation[AA]]) extends DerivableDerivation[ArgumentsSet[AA],GenTraversable[AA]] {
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
  def derivationId = Identifier[Derivation[Traversable[AA]]](WMHashHex("traversable" + xs.toSeq.map(_.derivationId).mkString))

  def description = ("Traversable(" + xs.map(_.description) + ")").limitAtWhitespace(80, "...")

  //def dependencies = xs.toSet
  def apply(args: ArgumentsSet[AA]) = {
    val argValues = args.resolved.values
    SuccessfulProvenance[GenTraversable[AA]](Identifier[Provenance[GenTraversable[AA]]](UUID.randomUUID().toString),
      derivationId, ProvenanceStatus.Success,
      derivedFromUnnamed = argValues, //derivedFromUnnamed.toSet.seq,
      output = Some(new GenTraversableArtifact(argValues.map(_.artifact))))
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
