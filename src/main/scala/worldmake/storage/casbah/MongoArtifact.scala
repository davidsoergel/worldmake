package worldmake.casbah

import worldmake._
import scala.collection.mutable
import com.mongodb.casbah.commons.Imports
import com.mongodb.casbah.Imports._
import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.util.Hash
import java.net.URL

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
      case MongoIntegerArtifact.typehint => new MongoIntegerArtifact(dbo).some
      case MongoDoubleArtifact.typehint => new MongoDoubleArtifact(dbo).some
      case MongoExternalPathArtifact.typehint => new MongoExternalPathArtifact(dbo).some
      case _ => None
    }
  }

  def artifactToDb(e: Artifact[_]): MongoWrapper = e match {
    case e: StringArtifact => MongoStringArtifact.toDb(e)
    case e: IntegerArtifact => MongoIntegerArtifact.toDb(e)
    case e: DoubleArtifact => MongoDoubleArtifact.toDb(e)
    case e: ExternalPathArtifact => MongoExternalPathArtifact.toDb(e)
  }
  
  def addFields(e: Artifact[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    //builder += "id" -> e.derivationId
    
    // must do this is each concrete class, since we don't know how the value is to be serialized.
    // or, add serializers for each type to the casbah mapping...
    //builder += "value" -> e.value
    builder += "contentHash" -> e.contentHash
  }
}


object MongoIntegerArtifact extends MongoSerializer[IntegerArtifact, MongoIntegerArtifact]("i", new MongoIntegerArtifact(_)) {
  def addFields(e: IntegerArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "value" -> e.value
  }
}

class MongoIntegerArtifact(val dbo: MongoDBObject) extends MongoArtifact[Integer] with IntegerArtifact with MongoWrapper {
  override def value = dbo.as[Integer]("value")
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


object MongoExternalPathArtifact extends MongoSerializer[ExternalPathArtifact, MongoExternalPathArtifact]("p", new MongoExternalPathArtifact(_)) {
  def addFields(e: ExternalPathArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "value" -> e.value.toURL
  }
}

class MongoExternalPathArtifact(val dbo: MongoDBObject) extends MongoArtifact[Path] with ExternalPathArtifact with MongoWrapper {
  override def value = Path.fromString(dbo.as[URL]("value").toExternalForm)
}
