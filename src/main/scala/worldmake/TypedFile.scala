package worldmake

import scalax.file.{LinkOption, Path}
import java.lang.Throwable
import scala.concurrent._
import worldmake.storage.Identifier

import ExecutionContext.Implicits.global
import scalax.file.defaultfs.DefaultPath
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.derivationstrategy.FutureDerivationStrategy
import scalax.file.PathMatcher

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


abstract class TypedPath(val path: Path) {
  // def pathType : String
  def validate(): Unit = {} // throws TypedFileValidationException

  def abspath = path.toAbsolute.path

  def basename = path.name
  
  //def toURL = path.toURL
  def exists = path.exists
  def nonExistent = path.nonExistent
  def isFile = path.isFile
  def fileOption = path.fileOption
  def children[U >: Path, F]() = path.children()
}

abstract class TypedFile(p: Path) extends TypedPath(p)

abstract class TypedDirectory(p: Path) extends TypedPath(p)


class TypedFileValidationException(message: String, cause: Throwable) extends Exception(message, cause)

abstract class BinaryFile(p: Path) extends TypedFile(p)

abstract class TextFile(p: Path) extends TypedFile(p)

object UnknownTypedPath extends TypedPathCompanion {
  def mapper = (p:Path) => new UnknownTypedPath(p)
}
class UnknownTypedPath(path: Path) extends TypedPath(path) with Logging {
 // val pathType = "unknown"
}

 
object BasicTextFile extends TypedPathCompanion{
//  private val wrapper: (Derivation[Path]) => TypedPathDerivation[BasicTextFile] = DerivationWrapper.wrapDerivation(new BasicTextFile(_))
  //implicit def wrapDerivation(d: Derivation[Path]): TypedPathDerivation[BasicTextFile] = wrapper(d)
  //TypedPathMapper.register("textfile", new BasicTextFile(_))

  def mapper = (p:Path) => new BasicTextFile(p)

  implicit def wrapDerivation(d: Derivation[Path]): TypedPathDerivation[BasicTextFile] = super.wrapDerivation(d)
}

class BasicTextFile(path: Path) extends TextFile(path) with Logging {
  //val pathType = "textfile"
}

abstract class CsvFile(p: Path) extends TextFile(p)

abstract class TsvFile(p: Path) extends TextFile(p)

trait TypedPathDerivation[+T <: TypedPath] extends Derivation[T] {
  def toPathDerivation: Derivation[Path]
}

object DerivationWrapper extends Logging {
  
  /*
  def pathFromString(ds:Derivation[String]):Derivation[Path] = new DerivableDerivation[Path]() {
    // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
    def derivationId = new Identifier("path"+ds.derivationId.s)

    def deriveFuture(implicit strategy: FutureDerivationStrategy): Future[Successful[TypedPath]] = {
      val pf = strategy.resolveOne(ds)
      pf.map(p => new Provenance[TypedPath] with Successful[TypedPath] {
        def output = p.output.map(a => new Artifact[TypedPath] {
          def contentHashBytes = a.contentHashBytes

          def value = {
            val result = Path.fromString(a.value)
            result
          }
        })

        def derivationId = new Identifier[Derivation[Path]](p.derivationId.s)

        def provenanceId = new Identifier(p.provenanceId.s)

        def status = p.status
      
    }
    )
    }

    def description = "Path: " + ds.description

    //def dependencies : A
    def dependencies = Set(ds)
  }*/

  private val namedPathFromString: IdentifiableFunction1[String, Path] = NamedFunction[String, Path]("pathFromString")((x: String) => Path.fromString(x))

  def pathFromString(ds:Derivation[String]):Derivation[Path] = new Derivation1(namedPathFromString,ds)
  
  
  def wrapDerivation[T <: TypedPath:ClassManifest](f: Path => T)(d: Derivation[Path]): TypedPathDerivation[T] = {
    new TypedPathDerivation[T] {
      def toPathDerivation = d
      private def pathType = classManifest[T].toString
      def derivationId = new Identifier(pathType+":"+d.derivationId.s)

      def description = d.description
      override def summary = d.summary
      override def setProvidedSummary(s:String) { d.setProvidedSummary(s) }

      def deriveFuture(implicit upstreamStrategy: FutureDerivationStrategy) : Future[Successful[T]] = {
        val pf = upstreamStrategy.resolveOne(d)
        val result = pf.map(p => wrapProvenance(p))
        result onFailure  {
          case t => {
            logger.debug("Error in Future: ", t)
          }
        }
        result
      }

      //def resolveOne = wrapProvenance(d.resolveOne)
        
      override val queue = d.queue
      
      private def wrapProvenance(p:Successful[Path]) = {
        new Provenance[T] with Successful[T] {
          
          // this Artifact must act like an ExternalPathArtifact, though it can't literally be one because TypedFile does not extend Path
          def output = new Artifact[T] { //with ExternalPathArtifact {
            def contentHashBytes = p.output.contentHashBytes

            def value : T = {
              val result = f(p.output.value)
              result.validate()
              result
            }

            def description = p.output.value.toAbsolute.path

            def abspath = p.output.value.toAbsolute.path

            def basename = p.output.value.name

            // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
            override def constantId = Identifier[Artifact[T]]("TypedPath(" + abspath + ")")


            // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
            // def /(s: String): ExternalPathArtifact = value / s
            override def environmentString = abspath
          }

          def derivationId = new Identifier[Derivation[T]](d.derivationId.s)

          def provenanceId = new Identifier(p.provenanceId.s)
        }
      }

      //def statusString = d.statusString
    }
  }
}
