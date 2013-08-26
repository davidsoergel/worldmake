package worldmake.casbah

import worldmake._
import scala.collection.mutable
import com.mongodb.casbah.commons.Imports
import com.mongodb.casbah.Imports._
import org.joda.time.DateTime
import scalax.file.Path
import worldmake.storage.Identifier

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait MongoProvenance[T] extends Provenance[T] with MongoWrapper {
  override def provenanceId = Identifier[Provenance[T]](dbo.as[String]("provenanceId"))

  override def output = dbo.getAs[DBObject]("output").map(MongoArtifact.artifactFromDb(_).asInstanceOf[Artifact[T]])

  override def status: ProvenanceStatus.ProvenanceStatus = ProvenanceStatus.withName(dbo.as[String]("status"))
}


object MongoProvenance {
  def addFields(e: Provenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    builder += "_id" -> e.provenanceId.s
    for (x <- e.output) {
      builder += "output" -> MongoArtifact.artifactToDb(x).dbo
    }
    builder += "status" -> e.status.toString
  }
}

object MongoConstantProvenance {
  val typehint = "ConstantProvenance"

  def toDb[T](e: ConstantProvenance[T]): MongoConstantProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoConstantProvenance[T](dbo)
  }

  def addFields(e: ConstantProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoProvenance.addFields(e, builder)
    builder += "createdTime" -> e.createdTime
  }
}

class MongoConstantProvenance[T](val dbo: MongoDBObject) extends MongoProvenance[T] with ConstantProvenance[T] with MongoWrapper {
  override def createdTime: DateTime = dbo.as[DateTime]("createdTime")

  override def status: ProvenanceStatus.ProvenanceStatus = super.status
  
  override def output: Some[Artifact[T]] = Some(MongoArtifact.artifactFromDb(dbo.as[DBObject]("output")).asInstanceOf[Artifact[T]])
}

object MongoDerivedProvenance {
  val typehint = "DerivedProvenance"

  def toDb[T](e: DerivedProvenance[T]): MongoDerivedProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    if (e.status == ProvenanceStatus.Success) {
      new MongoDerivedProvenance[T](dbo) with Successful[T]
    } else {
      new MongoDerivedProvenance[T](dbo)
    }
  }

  def addFields(e: DerivedProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoProvenance.addFields(e, builder)
    builder += "derivationId" -> e.derivationId
    builder += "derivedFromUnnamed" -> e.derivedFromUnnamed.map(_.provenanceId)
    builder += "derivedFromNamed" -> e.derivedFromNamed.mapValues(_.provenanceId)
    builder += "startTime" -> e.startTime
    builder += "endTime" -> e.endTime

    for (x <- e.exitCode) {
      builder += "exitCode" -> x
    }
    for (x <- e.log) {
      builder += "log" -> MongoStringOrFile.toDb(x).dbo
    }

    builder += "cost" -> e.cost.map({
      case (k, v) => (k.toString, v)
    })
  }
}

class MongoDerivedProvenance[T](val dbo: MongoDBObject) extends MongoProvenance[T] with DerivedProvenance[T] with MongoWrapper {
  //override def createdTime: DateTime = dbo.as[DateTime]("createdTime")
  //override def author : Option[String] =  dbo.getAs[String]("author")
  //override def notes : Option[String] =  dbo.getAs[String]("notes")

  override def derivationId: Identifier[Derivation[T]] = Identifier[Derivation[T]](dbo.as[String]("derivationId"))

  override def derivedFromUnnamed: Set[Provenance[_]] = dbo.as[Set[Provenance[_]]]("derivedFromUnnamed")  // todo wtf

  override def derivedFromNamed: Map[String, Provenance[_]] = dbo.as[Map[String, Provenance[_]]]("derivedFromNamed")

  override def startTime: DateTime = dbo.as[DateTime]("startTime")

  override def endTime: DateTime = dbo.as[DateTime]("endTime")

  override def exitCode: Option[Integer] = dbo.getAs[Integer]("exitCode")

  override def log: Option[ReadableStringOrFile] = dbo.getAs[DBObject]("log").map(new MongoStringOrFile(_))

  override def cost: Map[CostType.Value, Double] = dbo.as[MongoDBObject]("cost").map({
    case (k, v) => (CostType.withName(k), v.asInstanceOf[Double])
  }).toMap
}

object MongoStringOrFile {

  def toDb(e: ReadableStringOrFile): MongoStringOrFile = {
    val builder = MongoDBObject.newBuilder
    e.get.fold(s => {
      builder += "value" -> s
    }, p => {
      builder += "file" -> p.toAbsolute.path
    })

    val dbo = builder.result()
    new MongoStringOrFile(dbo)
  }


}

class MongoStringOrFile(val dbo: MongoDBObject) extends ReadableStringOrFile {
  def get: Either[String, Path] = {
    val sopt = dbo.getAs[String]("value").map(Left(_))
    sopt.getOrElse(Right(Path(dbo.as[String]("file"))))
  }
}
