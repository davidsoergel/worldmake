package worldmake

import scalax.file.Path
import java.lang.Throwable
import scala.concurrent._
import worldmake.storage.Identifier

import ExecutionContext.Implicits.global
import scalax.file.defaultfs.DefaultPath
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


abstract class TypedPath(val path: Path) {
  def validate(): Unit = {} // throws TypedFileValidationException
}

abstract class TypedFile(p: Path) extends TypedPath(p)

abstract class TypedDirectory(p: Path) extends TypedPath(p)


class TypedFileValidationException(message: String, cause: Throwable) extends Exception(message, cause)

abstract class BinaryFile(p: Path) extends TypedFile(p)

abstract class TextFile(p: Path) extends TypedFile(p)

abstract class CsvFile(p: Path) extends TextFile(p)

abstract class TsvFile(p: Path) extends TextFile(p)

trait TypedPathDerivation[T <: TypedPath] extends Derivation[T] {
  def toPathDerivation: Derivation[Path]
}

object DerivationWrapper extends Logging {
  
  /*
  def pathFromString(ds:Derivation[String]):Derivation[Path] = new DerivableDerivation[Path]() {
    // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
    def derivationId = new Identifier("path"+ds.derivationId.s)

    def deriveFuture(implicit strategy: FutureDerivationStrategy): Future[Successful[Path]] = {
      val pf = strategy.resolveOne(ds)
      pf.map(p => new Provenance[Path] with Successful[Path] {
        def output = p.output.map(a => new Artifact[Path] {
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
  
  
  def wrapDerivation[T <: TypedPath](f: Path => T)(d: Derivation[Path]): TypedPathDerivation[T] = {
    new TypedPathDerivation[T] {
      def toPathDerivation = d

      def derivationId = new Identifier(d.derivationId.s)

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
          def output = p.output.map(a => new Artifact[T] {
            def contentHashBytes = a.contentHashBytes

            def value = {
              val result = f(a.value)
              result.validate()
              result
            }
          })

          def derivationId = new Identifier[Derivation[T]](d.derivationId.s)

          def provenanceId = new Identifier(p.provenanceId.s)

          def status = p.status
        }
      }

      //def statusString = d.statusString
    }
  }
}
