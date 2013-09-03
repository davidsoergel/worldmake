package worldmake

import scalax.file.Path
import java.lang.Throwable
import scala.concurrent._
import worldmake.storage.Identifier

import ExecutionContext.Implicits.global
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

object DerivationWrapper {
  def wrapDerivation[T <: TypedPath](f: Path => T)(d: Derivation[Path]): TypedPathDerivation[T] = {
    new TypedPathDerivation[T] {
      def toPathDerivation = d

      def derivationId = new Identifier(d.derivationId.s)

      def description = d.description

      def resolveOneFuture: Future[Successful[T]] = {
        val pf = d.resolveOneFuture
        pf.map(p => wrapProvenance(p))
      }

      def resolveOne = wrapProvenance(d.resolveOne)
        
      
      
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

      def statusString = d.statusString
    }
  }
}
