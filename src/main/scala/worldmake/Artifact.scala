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
  //def artifactId: Identifier[Artifact[T]]

  def value: T

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

  implicit def fromPath(s: Path): Artifact[Path] = ExternalPathArtifact(s)
}
*/
object Artifact {
  def apply[T](v:T) : Artifact[T] = (v match {
    case s:String => StringArtifact(s)
    case i:Int => IntArtifact(i)
    case d:Double => DoubleArtifact(d)
    case p:Path => ExternalPathArtifact(p)
    case _ => throw new IllegalArtifactException(v.toString)
  }).asInstanceOf[Artifact[T]]
}

class IllegalArtifactException(message:String) extends Throwable(message)

object StringArtifact {
  def apply(s: String) = new MemoryStringArtifact(s) tap {(x:MemoryStringArtifact)=>Storage.provenanceStore.put(ConstantProvenance(x))}
}

trait StringArtifact extends Artifact[String] {
  def description = value.replace("\n","\\n").limitAtWhitespace(80, "...")
}

class MemoryStringArtifact(s: String) extends MemoryArtifact[String](s) with StringArtifact with ContentHashableArtifact[String] {
  //def contentHashBytes = WMHash(s)

  def output: Option[Artifact[String]] = Some(this)
}


object IntArtifact {
  def apply(s: Int) = new MemoryIntArtifact(s) //tap Storage.provenanceStore.put
}

trait IntArtifact extends Artifact[Int] {
  def description = value.toString

  override def constantId = Identifier[Artifact[Int]](WMHashHex("Int(" + value.toString + ")")) //perf
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

  override def constantId = Identifier[Artifact[Double]]("Double(" + value.toString + ")")
}

class MemoryDoubleArtifact(s: Double) extends MemoryArtifact[Double](s) with DoubleArtifact with ContentHashableArtifact[Double] {
  //def contentHashBytes = WMHash(s.toString)
  def output: Option[Artifact[Double]] = Some(this)

}


object ExternalPathArtifact {
  def apply(s: Path) = new MemoryExternalPathArtifact(s) //tap Storage.provenanceStore.put

}

trait ExternalPathArtifact extends Artifact[Path] {
  def description = value.toAbsolute.path

  def abspath = value.toAbsolute.path

  def basename = value.name
  
  // could be a complete serialization, or a UUID for an atomic artifact, or a hash of dependency IDs, etc.
  override def constantId = Identifier[Artifact[Path]]("Path(" + abspath + ")")


  // Navigating inside an artifact is a derivation; it shouldn't be possible to do it in the raw sense
  // def /(s: String): ExternalPathArtifact = value / s

}

class MemoryExternalPathArtifact(path: Path) extends MemoryArtifact[Path](path) with ExternalPathArtifact with Logging  with ContentHashableArtifact[Path] {
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
  private lazy val children: Seq[ExternalPathArtifact] = {
    //val isf = path.isFile
    if (path.nonExistent || path.isFile) Nil else path.children().toSeq.filter(p=>{!WorldMakeConfig.ignoreFilenames.contains(p.name)}).sorted.map(new MemoryExternalPathArtifact(_))
  }
  /*
  override def /(s: String): ExternalPathArtifact = new MemoryExternalPathArtifact(path / s)
 */

  def output: Option[ExternalPathArtifact] = Some(this)

}



class GenTraversableArtifact[T](val artifacts: GenTraversable[Artifact[T]]) extends Artifact[GenTraversable[T]] {
  //def provenanceId = Identifier[Artifact[Traversable[T]]](UUID.randomUUID().toString)

  def contentHashBytes = artifacts.toSeq.map(_.contentHashBytes).flatten.toArray

  lazy val value = artifacts.map(_.value)
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
