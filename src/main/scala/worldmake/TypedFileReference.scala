package worldmake

import scalax.file.{LinkOption, Path}
import java.lang.Throwable
import scala.concurrent._
import worldmake.storage._

import ExecutionContext.Implicits.global
import scalax.file.defaultfs.DefaultPath
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.cookingstrategy.CookingStrategy
import scalax.file.PathMatcher
import worldmake.storage.Identifier

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


abstract class TypedPathReference(val pathref: PathReference) {
  // def pathType : String
  def validate(): Unit = {} // throws TypedFileValidationException
  lazy val path = pathref.path
  lazy val abspath = path.toAbsolute.path

  lazy val basename = path.name
  
  //def toURL = path.toURL
  lazy val exists = path.exists
  lazy val nonExistent = path.nonExistent
  lazy val isFile = path.isFile
  lazy val fileOption = path.fileOption
  def children[U >: PathReference, F]() = path.children()
}

abstract class TypedFileReference(p: PathReference) extends TypedPathReference(p)

abstract class TypedDirectoryReference(p: PathReference) extends TypedPathReference(p)


class TypedFileValidationException(message: String, cause: Throwable) extends Exception(message, cause)

abstract class BinaryFile(p: PathReference) extends TypedFileReference(p)

abstract class TextFile(p: PathReference) extends TypedFileReference(p)

object UnknownTypedPathReference extends TypedPathCompanion {
  def mapper = (p:PathReference) => new UnknownTypedPathReference(p)
}
class UnknownTypedPathReference(path: PathReference) extends TypedPathReference(path) with Logging {
 // val pathType = "unknown"
}

 
object BasicTextFile extends TypedPathCompanion{
//  private val wrapper: (Derivation[Path]) => TypedPathDerivation[BasicTextFile] = DerivationWrapper.wrapDerivation(new BasicTextFile(_))
  //implicit def wrapDerivation(d: Derivation[Path]): TypedPathDerivation[BasicTextFile] = wrapper(d)
  //TypedPathMapper.register("textfile", new BasicTextFile(_))

  def mapper = (p:PathReference) => new BasicTextFile(p)

  implicit def wrapRecipe(d: Recipe[PathReference]): TypedPathRecipe[BasicTextFile] = super.wrapRecipe[BasicTextFile](d)
}

class BasicTextFile(path: PathReference) extends TextFile(path) with Logging {
  //val pathType = "textfile"
}

abstract class CsvFile(p: PathReference) extends TextFile(p)

abstract class TsvFile(p: PathReference) extends TextFile(p)

trait TypedPathRecipe[+T <: TypedPathReference] extends Recipe[T] {
  def toPathRecipe: Recipe[PathReference]
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

  private val namedPathFromString: IdentifiableFunction1[String, ExternalPath] = NamedFunction[String, ExternalPath]("pathFromString")((x: String) => ExternalPath(Path.fromString(x)))

  def externalPathFromString(ds:Recipe[String]):Recipe[ExternalPath] = new Recipe1(namedPathFromString,ds)
  
  def makePathRecipeTyped[T <: TypedPathReference:ClassManifest](f: PathReference => T)(d: Recipe[PathReference]): TypedPathRecipe[T] = {
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
        val result = pf.map(p => makePathProvenanceTyped(p))
        result onFailure  {
          case t => {
            logger.debug("Error in Future: ", t)
          }
        }
        result
      }

      //def resolveOne = wrapProvenance(d.resolveOne)
        
      override lazy val queue = d.queue
      
      private def makePathProvenanceTyped(p:Successful[PathReference]) = {
        new Provenance[T] with Successful[T] {
          def status = p.status
          def createdTime = p.createdTime
          // this Artifact must act like an ExternalPathArtifact, though it can't literally be one because TypedFile does not extend Path
          def output = new TypedPathReferenceArtifact(p,f)
          lazy val recipeId = new Identifier[Recipe[T]](d.recipeId.s)

          lazy val provenanceId = new Identifier(p.provenanceId.s)

          def derivedFromNamed = p.derivedFromNamed

          def derivedFromUnnamed = p.derivedFromUnnamed

          def cost = p.cost

          def endTime = p.endTime

          def enqueuedTime = p.enqueuedTime

          def exitCode = p.exitCode

          def log = p.log

          def runningInfo = p.runningInfo

          def startTime = p.startTime
        }
      }

      //def statusString = d.statusString
    }
  }
}

class TypedExternalPathArtifact[T <: TypedPathReference](_p:Successful[ExternalPath], _f: ExternalPath => T) extends TypedPathReferenceArtifact[T,ExternalPath](_p,_f) {

  val asPathArtifact : ExternalPathArtifact = TypedExternalPathArtifact(pathValue)
}
class TypedManagedPathArtifact[T <: TypedPathReference](_p:Successful[ManagedPath], _f: ManagedPath => T) extends TypedPathReferenceArtifact[T,ManagedPath](_p,_f) {

  val asPathArtifact : ManagedPathArtifact = TypedManagedPathArtifact(pathValue)
}


/*{ //with ExternalPathArtifact {
def contentHashBytes = p.output.contentHashBytes

  val pathValue = p.output.value
  val asPathArtifact : ExternalPathArtifact = ExternalPathArtifact(pathValue)

  lazy val value : T = {
    val result = f(pathValue)
    result.validate()
    result
  }

  //def description = p.output.value.toAbsolute.path

  lazy val abspath = p.output.value.path.toAbsolute.path

  //def basename = p.output.value.name

  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  override lazy val constantId = Identifier[Artifact[T]]("TypedPath(" + abspath + ")")


  // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
  // def /(s: String): ExternalPathArtifact = value / s
  override lazy val environmentString = abspath
}
*/

class TypedPathReferenceArtifact[T <: TypedPathReference, R <: PathReference](p:Successful[R], f: R => T) extends Artifact[T] { //with ExternalPathArtifact {
def contentHashBytes = p.output.contentHashBytes

  val pathValue = p.output.value

  lazy val value : T = {
    val result = f(pathValue)
    result.validate()
    result
  }

  //def description = p.output.value.toAbsolute.path

  lazy val abspath = p.output.value.path.toAbsolute.path

  //def basename = p.output.value.name

  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  override lazy val constantId = Identifier[Artifact[T]]("TypedPath(" + abspath + ")")


  // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
  // def /(s: String): ExternalPathArtifact = value / s
  override lazy val environmentString = abspath
}
