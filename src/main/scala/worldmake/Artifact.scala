package worldmake


import java.io.InputStream
import edu.umass.cs.iesl.scalacommons.util.Hash
import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.Tap._
import WorldMakeConfig.WMHash
import WorldMakeConfig.WMHashHex
import worldmake.storage.{Storage, Identifier}
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.GenTraversable
import edu.umass.cs.iesl.scalacommons.StringUtils
import StringUtils._
import scala.collection.mutable

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

/*
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
*/

trait Artifact[+T] extends Hashable {
  def infoBlock: String

  //def artifactId: Identifier[Artifact[T]]
  
  //def resultType : String // redundant with type parameter :(

  def value: T

  def environmentString: String //= value.toString  // dangerous to have a default; just provide explicitly in each concrete class
  
  //def contentHashBytes: Array[Byte]


  // An Artifact may be wrapped in a ConstantProvenance, so it's helpful for it to provide an ID up front
  // that is: this is the ID that should be used when the artifact is stored as a constant.  If it is stored as a derivation, then this zhourd be ignored.
  def constantId : Identifier[Artifact[T]] = Identifier[Artifact[T]](contentHash)
}


//trait InputArtifact[T] extends Artifact[T]
/*
trait InputArtifact[T] extends Artifact[T] { //with ConstantProvenance[T] {
  def value: T

  def resolve = this

  def status = Constant

  //def derivationId = Identifier[Derivation[T]](artifactId.s)

  // A ConstantArtifact will be automatically wrapped in a Provenance, so it's helpful for it to provide an ID up front
  def provenanceId : Identifier[Provenance[T]]
}*/

abstract class MemoryArtifact[T](val value: T) extends Artifact[T]//extends ConstantArtifact[T]

// this is mostly useful for making constants Hashable
/*
object ConstantArtifact {

  implicit def fromString(s: String): Artifact[String] =StringArtifact(s)

  implicit def fromDouble(s: Double): Artifact[Double] = DoubleArtifact(s)

  implicit def fromInt(s: Int): Artifact[Int] = IntArtifact(s)

  implicit def fromInt(s: Int): Artifact[Int] = IntArtifact(s)

  implicit def fromPath(s: Path): Artifact[TypedPath] = ExternalPathArtifact(s)
}
*/
object Artifact {
  def apply[T](v:T) : Artifact[T] = (v match {
    case s:String => StringArtifact(s)
    case i:Int => IntArtifact(i)
    case d:Double => DoubleArtifact(d)
    case p:Path => PathArtifact(p)
    case _ => throw new IllegalArtifactException(v.toString)
  }).asInstanceOf[Artifact[T]]
}

class IllegalArtifactException(message:String) extends Throwable(message)

object StringArtifact {
  def apply(s: String) = new MemoryStringArtifact(s) // tap {(x:MemoryStringArtifact)=>Storage.provenanceStore.put(ConstantProvenance(x))}
}

trait StringArtifact extends Artifact[String] {
  def description = value.replace("\n","\\n").limitAtWhitespace(80, "...")

  override def environmentString: String = value.toString
  //def resultType = "String"

  def infoBlock = s"         Value: ${value}\n"
}

class MemoryStringArtifact(s: String) extends MemoryArtifact[String](s) with StringArtifact with ContentHashableArtifact[String] {
  //def contentHashBytes = WMHash(s)

  def output: Option[Artifact[String]] = Some(this)
}


object BooleanArtifact {
  def apply(s: Boolean) = new MemoryBooleanArtifact(s) //tap Storage.provenanceStore.put
}

trait BooleanArtifact extends Artifact[Boolean] {
  def description = value.toString

  //def resultType = "Int"
  override def constantId = Identifier[Artifact[Boolean]](WMHashHex("Boolean(" + value.toString + ")")) //perf

  override def environmentString: String = value.toString

  def infoBlock = s"         Value: ${value}\n"
}

class MemoryBooleanArtifact(s: Boolean) extends MemoryArtifact[Boolean](s) with BooleanArtifact with ContentHashableArtifact[Boolean] {
  //def contentHashBytes = WMHash(s.toString)
  def output: Option[Artifact[Boolean]] = Some(this)
}



object IntArtifact {
  def apply(s: Int) = new MemoryIntArtifact(s) //tap Storage.provenanceStore.put
}

trait IntArtifact extends Artifact[Int] {
  def description = value.toString

  //def resultType = "Int"
  override def constantId = Identifier[Artifact[Int]](WMHashHex("Int(" + value.toString + ")")) //perf

  override def environmentString: String = value.toString
  def infoBlock = s"         Value: ${value}\n"
}

class MemoryIntArtifact(s: Int) extends MemoryArtifact[Int](s) with IntArtifact with ContentHashableArtifact[Int] {
  //def contentHashBytes = WMHash(s.toString)
  def output: Option[Artifact[Int]] = Some(this)
}


object DoubleArtifact {
  def apply(s: Double) = new MemoryDoubleArtifact(s) //tap Storage.provenanceStore.put
}

trait DoubleArtifact extends Artifact[Double] {
  def description = value.toString
  //def resultType = "Double"

  override def constantId = Identifier[Artifact[Double]]("Double(" + value.toString + ")")
  override def environmentString: String = value.toString
  def infoBlock = s"         Value: ${value}\n"
}

class MemoryDoubleArtifact(s: Double) extends MemoryArtifact[Double](s) with DoubleArtifact with ContentHashableArtifact[Double] {
  //def contentHashBytes = WMHash(s.toString)
  def output: Option[Artifact[Double]] = Some(this)

}


object PathArtifact {
  def apply(s: Path) : PathArtifact = new MemoryPathArtifact(s) //tap Storage.provenanceStore.put

}

trait PathArtifact extends Artifact[Path] {
  // def description //= value.abspath

  def abspath = value.toAbsolute.path

  def basename = value.name

  //def pathType : String //= value.pathType

  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  override def constantId = Identifier[Artifact[Path]]("Path(" + abspath + ")")


  // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
  // def /(s: String): ExternalPathArtifact = value / s
  override def environmentString = abspath

  def infoBlock = s"         Value: ${abspath}\n"
}

/*
object TypedPathArtifact {
  def apply[T <: TypedPath: ClassManifest](s: T) : Artifact[T] = new MemoryTypedPathArtifact[T](s) //tap Storage.provenanceStore.put

}

trait TypedPathArtifact extends Artifact[TypedPath] {
 // def description //= value.abspath

  def abspath = value.abspath

  def basename = value.basename
  
  //def pathType : String //= value.pathType
  
  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  override def constantId = Identifier[Artifact[TypedPath]]("Path(" + abspath + ")")


  // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
  // def /(s: String): ExternalPathArtifact = value / s
  override def environmentString = abspath
}
*/

class MemoryPathArtifact(path: Path) extends MemoryArtifact[Path](path) with PathArtifact with Logging  with ContentHashableArtifact[Path] {
  // ** require(path.exists)
  
  if(!path.exists) {
    logger.warn("External path artifact does not exist: " + path)
  }
  
  require(!path.toAbsolute.path.endsWith(".hg"))
  //   If it's a directory, this should in some sense include all the files in it (maybe just tgz?)-- but be careful about ignoring irrelevant metadata.
  /*override protected def bytesForContentHash = if (path.isFile) new FileInputStream(path.fileOption.get)
  else {
    path.children()
  }*/
  override lazy val contentHashBytes: Array[Byte] = if (path.isFile) WMHash(path.fileOption.get)
  else {
    // this doesn't take into account the filenames directly, but does concatenate the child hashes in filename-sorted order. 
    WMHash(children.map(p=> p.basename + p.contentHash).mkString)
  }
  //override 
  private lazy val children: Seq[PathArtifact] = {
    //val isf = path.isFile
    if (path.nonExistent || path.isFile) Nil else path.children().toSeq.filter(p=>{!WorldMakeConfig.ignoreFilenames.contains(p.name)}).sorted.map(f=>new MemoryPathArtifact(f))
  }
  /*
  override def /(s: String): ExternalPathArtifact = new MemoryExternalPathArtifact(path / s)
 */

  def output: Option[PathArtifact] = Some(this)

  //def pathType = classManifest[T].toString
}

trait TypedPathCompanion {
  def mapper : Path=>TypedPath

  private def wrapper[T<:TypedPath:ClassManifest]: (Recipe[Path]) => TypedPathRecipe[T] = RecipeWrapper.wrapRecipe[T](p=>mapper(p).asInstanceOf[T])

  //implicit
  def wrapRecipe[T<: TypedPath:ClassManifest](d: Recipe[Path]): TypedPathRecipe[T] = {
    val w = wrapper[T]
    w(d)
  }

  //def wrapper: (Derivation[Path]) => TypedPathDerivation[T] = DerivationWrapper.wrapDerivation[T](mapper)
}


/*
object TypedPathMapper {
  /*val typeMappings : mutable.Map[String, Path=>TypedPath] = mutable.HashMap[String, Path=>TypedPath]()
   
  def register[T<:TypedPathCompanion:ClassManifest](t:T) {
    val toType = classManifest[T].getClass.getName
    typeMappings.put(toType,t.mapper)
  }*/
 /* 
  def register(pathType:String, mapper:Path=>TypedPath) = {
    typeMappings.put(pathType,mapper)
  }
*/
  //def map[T <: TypedPath](pathType:String,file:Path) : T = typeMappings(pathType)(file).asInstanceOf[T]


  import scala.reflect.runtime.{universe => ru}
  val mirror = ru.runtimeMirror(getClass.getClassLoader)
  def map[T <: TypedPath](pathType:String,file:Path) :T = {
    val clz = Class.forName(pathType)
    val classSymbol = mirror.classSymbol(clz)
    val cType = classSymbol.toType
    val cm = mirror.reflectClass(classSymbol)
    val ctorC = cType.declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctorC)

    // the cast asserts that the file type requested via the "pathType" string was actually mapped to the right type
    ctorm(file).asInstanceOf[T]
    
    //val const = Class.forName("pathType").getConstructor(Class[Path])
  }

}
*/

/*object GenTraversableArtifact {
  def resultType = "Traversable"
}*/
trait GenTraversableArtifact[T] extends Artifact[GenTraversable[Artifact[T]]] {
  //def artifacts: GenTraversable[Artifact[T]]
  //lazy val value = artifacts.map(_.value)


  // ugly
  def infoBlock = s"Value : ${value.map(_.infoBlock).mkString("\n")} \n"
  
  override def environmentString = value.map(_.environmentString).mkString(" ")
}

class MemoryGenTraversableArtifact[T](val value: GenTraversable[Artifact[T]]) extends GenTraversableArtifact[T] {
  //def provenanceId = Identifier[Artifact[Traversable[T]]](UUID.randomUUID().toString)

  def contentHashBytes = WMHash(value.toSeq.flatMap(_.contentHash).mkString(""))


  //override def environmentString = value.map(_.environmentString).mkString(" ")
  //def resultType = GenTraversableArtifact.resultType

}

/*
trait VersionedImmutableInput[T]  extends InputArtifact[T]{
  def getVersion(v:String) : Derivation[T]
  def getLatestVersion : String
}

trait TimestampedMutableInput[T] extends InputArtifact[T] {

}

class UrlInputArtifact[T](url:URL) extends TimestampedMutableInput[T] {

}
*/
