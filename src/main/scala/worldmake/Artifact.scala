package worldmake


import java.io.InputStream
import edu.umass.cs.iesl.scalacommons.util.Hash
import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.Tap._
import worldmake.WorldMakeConfig._
import worldmake.storage.{ExternalPathArtifact, ManagedPathArtifact, Storage, Identifier}
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.GenTraversable
import edu.umass.cs.iesl.scalacommons.StringUtils
import StringUtils._
import scala.collection.mutable
import scala.Some

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

trait Artifact[+T] extends Hashable with WorldmakeEntity {
  
  //def artifactId: Identifier[Artifact[T]]
  
  //def resultType : String // redundant with type parameter :(

  def value: T

  def environmentString: String //= value.toString  // dangerous to have a default; just provide explicitly in each concrete class
  
  //def contentHashBytes: Array[Byte]


  // An Artifact may be wrapped in a ConstantProvenance, so it's helpful for it to provide an ID up front
  // that is: this is the ID that should be used when the artifact is stored as a constant.  If it is stored as a derivation, then this should be ignored.
  def constantId : Identifier[Artifact[T]] 
  
  override def toString = value.toString
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
    case pid:ManagedPath => ManagedPathArtifact(pid)
    case p:ExternalPath => ExternalPathArtifact(p)
    case t:GenTraversable[T] => new MemoryGenTraversableArtifact[T](t.map(Artifact(_)))
    case _ => throw new IllegalArtifactException(s"${v.getClass} : ${v.toString}")
  }).asInstanceOf[Artifact[T]]
}

object IllegalArtifactException {
  def apply(message: String): IllegalArtifactException = new IllegalArtifactException(message)
  def apply(message: String,cause: Throwable) = new IllegalArtifactException(message).initCause(cause)
}

class IllegalArtifactException(message: String) extends Exception(message)


object StringArtifact {
  def apply(s: String) = new MemoryStringArtifact(s) // tap {(x:MemoryStringArtifact)=>Storage.provenanceStore.put(ConstantProvenance(x))}
}

trait StringArtifact extends Artifact[String] {
  lazy val description = value.replace("\n","\\n").limitAtWhitespace(80, "...")

  override lazy val environmentString: String = value.toString
  //def resultType = "String"

  override lazy val constantId = Identifier[StringArtifact](WMHashHex("String(" + value.toString + ")"))
}

class MemoryStringArtifact(s: String) extends MemoryArtifact[String](s) with StringArtifact { //} with ContentHashableArtifact[String] {
  def contentHashBytes = Some(WMHash(s))

  lazy val output: Option[Artifact[String]] = Some(this)

  // An Artifact may be wrapped in a ConstantProvenance, so it's helpful for it to provide an ID up front
}


object BooleanArtifact {
  def apply(s: Boolean) = new MemoryBooleanArtifact(s) //tap Storage.provenanceStore.put
}

trait BooleanArtifact extends Artifact[Boolean] {
  lazy val description = value.toString

  //def resultType = "Int"
  override lazy val constantId = Identifier[Artifact[Boolean]](WMHashHex("Boolean(" + value.toString + ")")) //perf

  override lazy val environmentString: String = value.toString

}

class MemoryBooleanArtifact(s: Boolean) extends MemoryArtifact[Boolean](s) with BooleanArtifact { //with ContentHashableArtifact[Boolean] {
  def contentHashBytes = Some(WMHash(s.toString))
  lazy val output: Option[Artifact[Boolean]] = Some(this)
}



object IntArtifact {
  def apply(s: Int) = new MemoryIntArtifact(s) //tap Storage.provenanceStore.put
}

trait IntArtifact extends Artifact[Int] {
  lazy val description = value.toString

  //def resultType = "Int"
  override lazy val constantId = Identifier[Artifact[Int]](WMHashHex("Int(" + value.toString + ")")) //perf

  override lazy val environmentString: String = value.toString
}

class MemoryIntArtifact(s: Int) extends MemoryArtifact[Int](s) with IntArtifact { //with ContentHashableArtifact[Int] {
  def contentHashBytes = Some(WMHash(s.toString))
  lazy val output: Option[Artifact[Int]] = Some(this)
}


object DoubleArtifact {
  def apply(s: Double) = new MemoryDoubleArtifact(s) //tap Storage.provenanceStore.put
}

trait DoubleArtifact extends Artifact[Double] {
  lazy val description = value.toString
  //def resultType = "Double"

  override lazy val constantId = Identifier[Artifact[Double]]("Double(" + value.toString + ")")
  override lazy val environmentString: String = value.toString
}

class MemoryDoubleArtifact(s: Double) extends MemoryArtifact[Double](s) with DoubleArtifact { //with ContentHashableArtifact[Double] {
  def contentHashBytes = Some(WMHash(s.toString))
  lazy val output: Option[Artifact[Double]] = Some(this)

}



/*object GenTraversableArtifact {
  def resultType = "Traversable"
}*/
trait GenTraversableArtifact[T] extends Artifact[GenTraversable[Artifact[T]]] {
  //def artifacts: GenTraversable[Artifact[T]]
  //lazy val value = artifacts.map(_.value)

  override def toString = value.map(_.toString).mkString(", ")
  
  override lazy val environmentString = value.map(_.environmentString).mkString(" ")

  override def constantId = Identifier[Artifact[GenTraversable[Artifact[T]]]](WMHashHex(value.map(_.constantId.s).mkString(", "))) //contentHash.get)

}

class MemoryGenTraversableArtifact[T](val value: GenTraversable[Artifact[T]]) extends GenTraversableArtifact[T] {
  //def provenanceId = Identifier[Artifact[Traversable[T]]](UUID.randomUUID().toString)

  lazy val contentHashBytes = Some(WMHash(value.toSeq.flatMap(_.contentHash).mkString("")))


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
