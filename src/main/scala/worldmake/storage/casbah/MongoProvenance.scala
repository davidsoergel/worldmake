package worldmake.storage.casbah

import worldmake._
import scala.collection.mutable
import com.mongodb.casbah.commons.Imports
import com.mongodb.casbah.Imports._
import org.joda.time.DateTime
import scalax.file.Path
import worldmake.storage.{Storage, Identifier}

/*
// don't use, because of parameterized types

abstract class MongoSerializer[E, D](val typehint: String, constructor: MongoDBObject => D) {
  def toDb(e: E): D = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    constructor(dbo)
  }

  def addFields(e: E, builder: mutable.Builder[(String, Any), Imports.DBObject])
}
*/

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait MongoProvenance[T] extends Provenance[T] with MongoWrapper {
  override lazy val provenanceId = Identifier[Provenance[T]](dbo.as[String]("_id"))
}

object MongoProvenance {
  def addFields(e: Provenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    builder += "_id" -> e.provenanceId.s
  }
}


trait MongoSuccessful[T] extends MongoProvenance[T] with Successful[T] with MongoWrapper {
  override def output: Artifact[T] = MongoArtifact.artifactFromDb(dbo.as[DBObject]("output")).asInstanceOf[Artifact[T]] //unsafe cast due to erasure

}

object MongoSuccessful {
  def addFields(e: Successful[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoProvenance.addFields(e, builder)
    builder += "output" -> MongoArtifact.artifactToDb(e.output).dbo
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
    MongoSuccessful.addFields(e, builder)
    builder += "createdTime" -> e.createdTime
  }
}

class MongoConstantProvenance[T](val dbo: MongoDBObject) extends MongoSuccessful[T] with ConstantProvenance[T] with MongoWrapper {
  override def createdTime: DateTime = dbo.as[DateTime]("createdTime")
}


trait MongoDerivedProvenance[T] extends MongoProvenance[T] with MongoWrapper {
  override lazy val recipeId = Identifier[Recipe[T]](dbo.as[String]("recipeId"))
}

object MongoDerivedProvenance {
  def addFields(e: DerivedProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoProvenance.addFields(e, builder)
    builder += "recipeId" -> e.recipeId.s
  }
}


trait MongoDependenciesBoundProvenance[T] extends MongoDerivedProvenance[T] with MongoWrapper {
  lazy val store = Storage.provenanceStore.asInstanceOf[CasbahProvenanceStore]
  def derivedFromUnnamed: Set[Successful[_]] = dbo.getAs[MongoDBList]("derivedFromUnnamed").getOrElse(List()).map(_.asInstanceOf[String]).toSet.flatMap((id: String) => store.getSuccessful[Any](new Identifier[Successful[_]](id)))
  def derivedFromNamed: Map[String, Successful[_]] = dbo.getAs[MongoDBObject]("derivedFromNamed").getOrElse(Map()).mapValues(id => store.getSuccessful[Any](new Identifier[Successful[_]](id.asInstanceOf[String])).get).toMap

  //def derivedFromAll = derivedFromUnnamed ++ derivedFromNamed.values
}

object MongoDependenciesBoundProvenance {
  def addFields(e: DependenciesBoundProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoDerivedProvenance.addFields(e, builder)
    if (e.derivedFromUnnamed.nonEmpty) {
      builder += "derivedFromUnnamed" -> e.derivedFromUnnamed.seq.map(_.provenanceId.s)
    }
    if (e.derivedFromNamed.nonEmpty) {
      builder += "derivedFromNamed" -> e.derivedFromNamed.seq.mapValues(_.provenanceId.s)
    }  
  }
}

object MongoBlockedProvenance {
  val typehint = "BlockedProvenance"

  def toDb[T](e: BlockedProvenance[T]): MongoBlockedProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoBlockedProvenance[T](dbo)
  }

  def addFields(e: BlockedProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoDerivedProvenance.addFields(e, builder)
    builder += "createdTime" -> e.createdTime
  }
}

class MongoBlockedProvenance[T](val dbo: MongoDBObject) extends MongoDerivedProvenance[T] with BlockedProvenance[T] with MongoWrapper {
  override def createdTime: DateTime = dbo.as[DateTime]("createdTime")
}

object MongoPendingProvenance {
  val typehint = "PendingProvenance"

  def toDb[T](e: PendingProvenance[T]): MongoPendingProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoPendingProvenance[T](dbo)
  }

  def addFields(e: PendingProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoDependenciesBoundProvenance.addFields(e, builder)
    builder += "createdTime" -> e.createdTime
    builder += "enqueuedTime" -> e.createdTime   
  }
}

class MongoPendingProvenance[T](val dbo: MongoDBObject) extends MongoDependenciesBoundProvenance[T] with PendingProvenance[T] with MongoWrapper {
  override def createdTime: DateTime = dbo.as[DateTime]("createdTime")

  override def enqueuedTime: DateTime = dbo.as[DateTime]("enqueuedTime")

}


object MongoRunningProvenance {
  val typehint = "RunningProvenance"

  def toDb[T](e: RunningProvenance[T]): MongoRunningProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoRunningProvenance[T](dbo)
  }

  def addFields(e: RunningProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoDependenciesBoundProvenance.addFields(e, builder)
    builder += "createdTime" -> e.createdTime
    builder += "enqueuedTime" -> e.createdTime
    if (e.derivedFromUnnamed.nonEmpty) {
      builder += "derivedFromUnnamed" -> e.derivedFromUnnamed.seq.map(_.provenanceId.s)
    }
    if (e.derivedFromNamed.nonEmpty) {
      builder += "derivedFromNamed" -> e.derivedFromNamed.seq.mapValues(_.provenanceId.s)
    }
    builder += "startTime" -> e.startTime
    builder += "runningInfo" -> MongoRunningInfo.toDb(e.runningInfo).dbo
  }
}

class MongoRunningProvenance[T](val dbo: MongoDBObject) extends MongoDependenciesBoundProvenance[T] with RunningProvenance[T] with MongoWrapper {
  override def createdTime: DateTime = dbo.as[DateTime]("createdTime")

  override def enqueuedTime: DateTime = dbo.as[DateTime]("enqueuedTime")

  override def startTime: DateTime = dbo.as[DateTime]("startTime")

  override def runningInfo: RunningInfo = MongoRunningInfo.fromDb(dbo.as[DBObject]("runningInfo"))
}


object MongoPostRunProvenance {
  /*
  val typehint = "PostRunProvenance"

  def toDb[T](e: PostRunProvenance[T]): MongoPostRunProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoPostRunProvenance[T](dbo)
  }
  */

  def addFields(e: PostRunProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoDependenciesBoundProvenance.addFields(e, builder)
    builder += "createdTime" -> e.createdTime
    builder += "enqueuedTime" -> e.createdTime
    builder += "startTime" -> e.startTime
    builder += "runningInfo" -> MongoRunningInfo.toDb(e.runningInfo).dbo
    builder += "endTime" -> e.endTime
    builder += "exitCode" -> e.exitCode
    for (x <- e.log) {
      builder += "log" -> MongoStringOrManagedFile.toDb(x).dbo
    }
    if (e.cost.nonEmpty) {
      builder += "cost" -> e.cost.map({
        case (k, v) => (k.toString, v)
      })
    }
  }
}

abstract class MongoPostRunProvenance[T](val dbo: MongoDBObject) extends MongoDependenciesBoundProvenance[T] with PostRunProvenance[T] with MongoWrapper {
  override def createdTime: DateTime = dbo.as[DateTime]("createdTime")

  override def enqueuedTime: DateTime = dbo.as[DateTime]("enqueuedTime")

  override def startTime: DateTime = dbo.as[DateTime]("startTime")

  override def runningInfo: RunningInfo = MongoRunningInfo.fromDb(dbo.as[DBObject]("runningInfo"))

  override def endTime: DateTime = dbo.as[DateTime]("endTime")

  override def exitCode: Int = dbo.as[Int]("exitCode")

  override def log: Option[ReadableStringOrManagedFile] = dbo.getAs[DBObject]("log").map(new MongoStringOrManagedFile(_))

  override def cost: Map[CostType.Value, Double] = dbo.as[MongoDBObject]("cost").map({
    case (k, v) => (CostType.withName(k), v.asInstanceOf[Double])
  }).toMap

}


object MongoFailedProvenance {

  val typehint = "FailedProvenance"

  def toDb[T](e: FailedProvenance[T]): MongoFailedProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoFailedProvenance[T](dbo)
  }

  def addFields(e: FailedProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoPostRunProvenance.addFields(e, builder)
  }
}

class MongoFailedProvenance[T](val xdbo: MongoDBObject) extends MongoPostRunProvenance[T](xdbo) with FailedProvenance[T] with MongoWrapper


object MongoCancelledProvenance {

  val typehint = "CancelledProvenance"

  def toDb[T](e: CancelledProvenance[T]): MongoCancelledProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoCancelledProvenance[T](dbo)
  }

  def addFields(e: CancelledProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoPostRunProvenance.addFields(e, builder)
  }
}

class MongoCancelledProvenance[T](val xdbo: MongoDBObject) extends MongoPostRunProvenance[T](xdbo) with FailedProvenance[T] with MongoWrapper

object MongoCompletedProvenance {

  val typehint = "CompletedProvenance"

  def toDb[T](e: CompletedProvenance[T]): MongoCompletedProvenance[T] = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    new MongoCompletedProvenance[T](dbo)
  }

  def addFields(e: CompletedProvenance[_], builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoPostRunProvenance.addFields(e, builder)
    MongoSuccessful.addFields(e, builder)
  }
}

class MongoCompletedProvenance[T](val xdbo: MongoDBObject) extends MongoPostRunProvenance[T](xdbo) with MongoSuccessful[T] with CompletedProvenance[T] with MongoWrapper


object MongoStringOrManagedFile {

  def toDb(e: ReadableStringOrManagedFile): MongoStringOrManagedFile = {
    val builder = MongoDBObject.newBuilder
    e.get.fold(s => {
      if (s.nonEmpty) {
        builder += "value" -> s
      }
    }, p => {
      builder += "fileId" -> p.id
    })

    val dbo = builder.result()
    new MongoStringOrManagedFile(dbo)
  }


}

class MongoStringOrManagedFile(val dbo: MongoDBObject) extends ReadableStringOrManagedFile {
  def get: Either[String, ManagedPath] = {
    val sopt = dbo.getAs[String]("value").map(Left(_))
    sopt.getOrElse(Right(ManagedPath(Identifier[ManagedPath](dbo.as[String]("fileId")))))
  }
}
