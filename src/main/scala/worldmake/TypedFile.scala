package worldmake

import scalax.file.{LinkOption, Path}
import java.lang.Throwable
import scala.concurrent._
import worldmake.storage.Identifier

import ExecutionContext.Implicits.global
import scalax.file.defaultfs.DefaultPath
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.cookingstrategy.CookingStrategy
import scalax.file.PathMatcher

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


abstract class TypedPath(val path: Path) {
  // def pathType : String
  def validate(): Unit = {} // throws TypedFileValidationException

  lazy val abspath = path.toAbsolute.path

  lazy val basename = path.name
  
  //def toURL = path.toURL
  lazy val exists = path.exists
  lazy val nonExistent = path.nonExistent
  lazy val isFile = path.isFile
  lazy val fileOption = path.fileOption
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

  implicit def wrapRecipe(d: Recipe[Path]): TypedPathRecipe[BasicTextFile] = super.wrapRecipe[BasicTextFile](d)
}

class BasicTextFile(path: Path) extends TextFile(path) with Logging {
  //val pathType = "textfile"
}

abstract class CsvFile(p: Path) extends TextFile(p)

abstract class TsvFile(p: Path) extends TextFile(p)

trait TypedPathRecipe[+T <: TypedPath] extends Recipe[T] {
  def toPathRecipe: Recipe[Path]
}

object RecipeWrapper extends Logging {
  
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

  def pathFromString(ds:Recipe[String]):Recipe[Path] = new Recipe1(namedPathFromString,ds)
  
  def wrapRecipe[T <: TypedPath:ClassManifest](f: Path => T)(d: Recipe[Path]): TypedPathRecipe[T] = {
    new TypedPathRecipe[T] {
      def toPathRecipe = d
      private val pathType = classManifest[T].toString
      require(!pathType.equals("Nothing"))
      lazy val recipeId = new Identifier(d.recipeId.s + "(" + pathType + ")")

      lazy val longDescription = d.longDescription
      override lazy val summary = d.summary
      override def setProvidedSummary(s:String) { d.setProvidedSummary(s) }

      def deriveFuture(implicit upstreamStrategy: CookingStrategy) : Future[Successful[T]] = {
        val pf = upstreamStrategy.cookOne(d)
        val result = pf.map(p => wrapProvenance(p))
        result onFailure  {
          case t => {
            logger.debug("Error in Future: ", t)
          }
        }
        result
      }

      //def resolveOne = wrapProvenance(d.resolveOne)
        
      override lazy val queue = d.queue
      
      private def wrapProvenance(p:Successful[Path]) = {
        new Provenance[T] with Successful[T] {
          def createdTime = p.createdTime
          // this Artifact must act like an ExternalPathArtifact, though it can't literally be one because TypedFile does not extend Path
          def output = new TypedPathArtifact(p,f)
          lazy val recipeId = new Identifier[Recipe[T]](d.recipeId.s)

          lazy val provenanceId = new Identifier(p.provenanceId.s)
        }
      }

      //def statusString = d.statusString
    }
  }
}

class TypedPathArtifact[T <: TypedPath](p:Successful[Path], f: Path => T) extends Artifact[T] { //with ExternalPathArtifact {
def contentHashBytes = p.output.contentHashBytes

  val pathValue = p.output.value
  val asPathArtifact : PathArtifact = PathArtifact(pathValue)

  lazy val value : T = {
    val result = f(pathValue)
    result.validate()
    result
  }

  override lazy val infoBlock : String = p.infoBlock

  //def description = p.output.value.toAbsolute.path

  lazy val abspath = p.output.value.toAbsolute.path

  //def basename = p.output.value.name

  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  override lazy val constantId = Identifier[Artifact[T]]("TypedPath(" + abspath + ")")


  // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
  // def /(s: String): ExternalPathArtifact = value / s
  override lazy val environmentString = abspath
}
