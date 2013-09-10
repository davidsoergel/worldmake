package worldmake.storage.casbah

import worldmake._
import scala.collection.{mutable, GenTraversable}
import com.mongodb.casbah.commons.Imports
import com.mongodb.casbah.Imports._
import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.util.Hash

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait MongoArtifact[T] extends Artifact[T] with MongoWrapper {
  //override def artifactId = dbo.as[String]("id")

  override def contentHash = dbo.as[String]("contentHash")

  override def contentHashBytes = Hash.fromHex(contentHash)
}


object MongoArtifact {

  import scalaz.syntax.std.option._

  def artifactFromDb(dbo: MongoDBObject): Artifact[_] =
    artifactFromDbOpt(dbo).getOrElse(sys.error("cannot deserialize object of type '" + dbo("type") + "'"))

  def artifactFromDbOpt(dbo: MongoDBObject): Option[Artifact[_]] = {
    dbo("type") match {
      case MongoStringArtifact.typehint => new MongoStringArtifact(dbo).some
      case MongoIntArtifact.typehint => new MongoIntArtifact(dbo).some
      case MongoDoubleArtifact.typehint => new MongoDoubleArtifact(dbo).some
      case MongoPathArtifact.typehint => new MongoPathArtifact(dbo).some
      case MongoGenTraversableArtifact.typehint => new MongoGenTraversableArtifact(dbo).some
      case _ => None
    }
  }

  def artifactToDb(e: Artifact[_]): MongoWrapper = e match {
    case e: StringArtifact => MongoStringArtifact.toDb(e)
    case e: IntArtifact => MongoIntArtifact.toDb(e)
    case e: DoubleArtifact => MongoDoubleArtifact.toDb(e)
    case e: PathArtifact => MongoPathArtifact.toDb(e)
    case e: TypedPathArtifact[_] => MongoPathArtifact.toDb(e.asPathArtifact)  // stored without the typing info.  This will be re-wrapped every time it is loaded.  Careful about redundant verification.
    case e: GenTraversableArtifact[_] => MongoGenTraversableArtifact.toDb(e)
    /*case e: Artifact => e.value match {
      case f: String => MongoStringArtifact.toDb(e)
      case f: Int => MongoIntArtifact.toDb(e)
      case f: Double => MongoDoubleArtifact.toDb(e)
      case f: Path => MongoExternalPathArtifact.toDb(e)
    }*/
  }
  
  def addFields(e: Artifact[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    //builder += "id" -> e.derivationId
    
    // must do this is each concrete class, since we don't know how the value is to be serialized.
    // or, add serializers for each type to the casbah mapping...
    //builder += "value" -> e.value
    builder += "contentHash" -> e.contentHash
  }
}


object MongoIntArtifact extends MongoSerializer[IntArtifact, MongoIntArtifact]("i", new MongoIntArtifact(_)) {
  def addFields(e: IntArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "value" -> e.value
  }
}

class MongoIntArtifact(val dbo: MongoDBObject) extends MongoArtifact[Int] with IntArtifact with MongoWrapper {
  override def value = dbo.as[Int]("value")
}


object MongoDoubleArtifact extends MongoSerializer[DoubleArtifact, MongoDoubleArtifact]("d", new MongoDoubleArtifact(_)) {
  def addFields(e: DoubleArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "value" -> e.value
  }
}

class MongoDoubleArtifact(val dbo: MongoDBObject) extends MongoArtifact[Double] with DoubleArtifact with MongoWrapper {
  override def value = dbo.as[Double]("value")
}


object MongoStringArtifact extends MongoSerializer[StringArtifact, MongoStringArtifact]("s", new MongoStringArtifact(_)) {
  def addFields(e: StringArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "value" -> e.value
  }
}

class MongoStringArtifact(val dbo: MongoDBObject) extends MongoArtifact[String] with StringArtifact with MongoWrapper {
  override def value = dbo.as[String]("value")
}


object MongoPathArtifact extends MongoSerializer[PathArtifact, MongoPathArtifact]("p", new MongoPathArtifact(_)) {
  def addFields(e: PathArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    //builder += "value" -> e.value.toURL
    builder += "value" -> e.abspath
    //builder += "pathType" -> e.pathType
  }
}

class MongoPathArtifact(val dbo: MongoDBObject) extends MongoArtifact[Path] with PathArtifact with MongoWrapper {

  //def pathType = dbo.as[String]("pathType")
  //override def value = TypedPathMapper.map( pathType, Path.fromString(dbo.as[URL]("value").toExternalForm))
  //override def value = Path.fromString(dbo.as[URL]("value").toExternalForm)
  override def value = Path.fromString(dbo.as[String]("value"))

}

object MongoGenTraversableArtifact extends MongoSerializer[GenTraversableArtifact[_], MongoGenTraversableArtifact[_]]("t", new MongoGenTraversableArtifact(_)) {
  def addFields(e: GenTraversableArtifact[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "value" -> e.value.map(x=>MongoArtifact.artifactToDb(x).dbo)
  }

}

class MongoGenTraversableArtifact[T](val dbo: MongoDBObject) extends MongoArtifact[GenTraversable[Artifact[T]]] with GenTraversableArtifact[T] with MongoWrapper {
  override def value = dbo.as[MongoDBList]("value").map(a => MongoArtifact.artifactFromDb(a.asInstanceOf[MongoDBObject]).asInstanceOf[Artifact[T]])
}
