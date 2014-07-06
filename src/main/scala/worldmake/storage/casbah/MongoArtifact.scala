/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.storage.casbah

import worldmake._
import scala.collection.{mutable, GenTraversable}
import com.mongodb.casbah.commons.Imports
import com.mongodb.casbah.Imports._
import scalax.file.Path
import edu.umass.cs.iesl.scalacommons.util.Hash
import worldmake.storage.{ManagedPathArtifact, ExternalPathArtifact, Identifier}
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait MongoArtifact[T] extends Artifact[T] with MongoWrapper {
  //override def artifactId = dbo.as[String]("id")

  override lazy val contentHash = dbo.getAs[String]("contentHash")

  override lazy val contentHashBytes = contentHash.map(Hash.fromHex)
}


object MongoArtifact {

  import scalaz.syntax.std.option._

  def artifactFromDb(dbo: MongoDBObject): Artifact[_] =
    artifactFromDbOpt(dbo).getOrElse(sys.error("cannot deserialize object of type '" + dbo("type") + "'"))

  def artifactFromDbOpt(dbo: MongoDBObject): Option[Artifact[_]] = {
    dbo("type") match {
      case MongoStringArtifact.typehint => new MongoStringArtifact(dbo).some
      case MongoIntArtifact.typehint => new MongoIntArtifact(dbo).some
      case MongoBooleanArtifact.typehint => new MongoBooleanArtifact(dbo).some
      case MongoDoubleArtifact.typehint => new MongoDoubleArtifact(dbo).some
      case MongoExternalPathArtifact.typehint => new MongoExternalPathArtifact(dbo).some
      case MongoManagedPathArtifact.typehint => new MongoManagedPathArtifact(dbo).some
      case MongoGenTraversableArtifact.typehint => new MongoGenTraversableArtifact(dbo).some
      case _ => None
    }
  }

  def artifactToDb(e: Artifact[_]): MongoWrapper = e match {
    case e: StringArtifact => MongoStringArtifact.toDb(e)
    case e: IntArtifact => MongoIntArtifact.toDb(e)
    case e: BooleanArtifact => MongoBooleanArtifact.toDb(e)
    case e: DoubleArtifact => MongoDoubleArtifact.toDb(e)
    case e: ExternalPathArtifact => MongoExternalPathArtifact.toDb(e)
    case e: ManagedPathArtifact => MongoManagedPathArtifact.toDb(e)
    case e: TypedExternalPathArtifact[_] => MongoExternalPathArtifact.toDb(e.asPathArtifact)  // stored without the typing info.  This will be re-wrapped every time it is loaded.  Careful about redundant verification.
    case e: TypedManagedPathArtifact[_] => MongoManagedPathArtifact.toDb(e.asPathArtifact)
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

    if (aggressiveHashing) {
      builder += "contentHash" -> e.contentHash.get
    }
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
object MongoBooleanArtifact extends MongoSerializer[BooleanArtifact, MongoBooleanArtifact]("b", new MongoBooleanArtifact(_)) {
  def addFields(e: BooleanArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "value" -> e.value
  }
}

class MongoBooleanArtifact(val dbo: MongoDBObject) extends MongoArtifact[Boolean] with BooleanArtifact with MongoWrapper {
  override def value = dbo.as[Boolean]("value")

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

object MongoExternalPathArtifact extends MongoSerializer[ExternalPathArtifact, MongoExternalPathArtifact]("ep", new MongoExternalPathArtifact(_)) {
  def addFields(e: ExternalPathArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    //builder += "value" -> e.value.toURL
    builder += "value" -> e.abspath
    //builder += "pathType" -> e.pathType
  }
}

class MongoExternalPathArtifact(val dbo: MongoDBObject) extends MongoArtifact[ExternalPath] with ExternalPathArtifact with MongoWrapper {

  //def pathType = dbo.as[String]("pathType")
  //override def value = TypedPathMapper.map( pathType, Path.fromString(dbo.as[URL]("value").toExternalForm))
  //override def value = Path.fromString(dbo.as[URL]("value").toExternalForm)
  override def value = ExternalPath(Path.fromString(dbo.as[String]("value")))

}


object MongoManagedPathArtifact extends MongoSerializer[ManagedPathArtifact, MongoManagedPathArtifact]("mp", new MongoManagedPathArtifact(_)) {
  def addFields(e: ManagedPathArtifact, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    //builder += "value" -> e.value.toURL
    builder += "value" -> e.value.id.s
    e.value.relative.map(x=>{
    builder += "rel" -> x.path
    })
    //builder += "pathType" -> e.pathType
  }
}

class MongoManagedPathArtifact(val dbo: MongoDBObject) extends MongoArtifact[ManagedPath] with ManagedPathArtifact with MongoWrapper {

  //def pathType = dbo.as[String]("pathType")
  //override def value = TypedPathMapper.map( pathType, Path.fromString(dbo.as[URL]("value").toExternalForm))
  //override def value = Path.fromString(dbo.as[URL]("value").toExternalForm)
  override def value = ManagedPath(Identifier[ManagedPath](dbo.as[String]("value")), dbo.getAs[String]("rel").map(Path.fromString))

}


object MongoGenTraversableArtifact extends MongoSerializer[GenTraversableArtifact[_], MongoGenTraversableArtifact[_]]("t", new MongoGenTraversableArtifact(_)) {
  def addFields(e: GenTraversableArtifact[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoArtifact.addFields(e, builder)
    builder += "artifacts" -> e.artifacts.map(x=>MongoArtifact.artifactToDb(x).dbo)
  }

}

class MongoGenTraversableArtifact[T](val dbo: MongoDBObject) extends MongoArtifact[GenTraversable[T]] with GenTraversableArtifact[T] with MongoWrapper {
  override def artifacts = dbo.as[MongoDBList]("artifacts").map(a => MongoArtifact.artifactFromDb(new MongoDBObject(a.asInstanceOf[BasicDBObject])).asInstanceOf[Artifact[T]])

  //override def environmentString = value.map(_.environmentString).mkString(" ")  // unclear why the super version doesn't work; looks like a trait order issue
}
