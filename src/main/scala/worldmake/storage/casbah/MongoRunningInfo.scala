package worldmake.storage.casbah

import worldmake._
import edu.umass.cs.iesl.scalacommons.util.Hash
import com.mongodb.casbah.Imports._
import scala.collection.mutable
import com.mongodb.casbah.commons.Imports
import worldmake.executionstrategy.QsubRunningInfo
import scalax.file.Path
import worldmake.storage.Identifier

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait MongoRunningInfo extends RunningInfo with MongoWrapper {
  override def node = dbo.getAs[String]("node")
}


object MongoRunningInfo  {

  import scalaz.syntax.std.option._

  def fromDb(dbo: MongoDBObject): RunningInfo  =
    fromDbOpt(dbo).getOrElse(sys.error("cannot deserialize object of type '" + dbo("type") + "'"))

  def fromDbOpt(dbo: MongoDBObject): Option[RunningInfo ] = {
    dbo("type") match {
      case MongoWithinJvmRunningInfo.typehint => new MongoWithinJvmRunningInfo(dbo).some
      case MongoLocalRunningInfo.typehint => new MongoLocalRunningInfo(dbo).some
      case MongoQsubRunningInfo.typehint => new MongoQsubRunningInfo(dbo).some
      case _ => None
    }
  }

  def toDb(e: RunningInfo): MongoWrapper = e match {
    case e: WithinJvmRunningInfo => MongoWithinJvmRunningInfo.toDb(e)
    case e: LocalRunningInfo => MongoLocalRunningInfo.toDb(e)
    case e: QsubRunningInfo => MongoQsubRunningInfo.toDb(e)
  }

  def addFields(e: RunningInfo, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    for(n <- e.node) {
    builder += "node" -> n
    }
  }
}


object MongoWithinJvmRunningInfo extends MongoSerializer[WithinJvmRunningInfo, MongoWithinJvmRunningInfo]("WithinJvmRunningInfo", new MongoWithinJvmRunningInfo(_)) {
  def addFields(e: WithinJvmRunningInfo, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoRunningInfo.addFields(e, builder)
  }
}

class MongoWithinJvmRunningInfo(val dbo: MongoDBObject) extends MongoRunningInfo with WithinJvmRunningInfo with MongoWrapper



object MongoLocalRunningInfo extends MongoSerializer[LocalRunningInfo, MongoLocalRunningInfo]("LocalRunningInfo", new MongoLocalRunningInfo(_)) {
  def addFields(e: LocalRunningInfo, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoRunningInfo.addFields(e, builder)
    builder += "workingDir" -> e.workingDir.toAbsolute.path
  }
}

class MongoLocalRunningInfo(val dbo: MongoDBObject) extends MongoRunningInfo with LocalRunningInfo with MongoWrapper{
  def workingDir = Path.fromString(dbo.as[String]("workingDir"))
}



object MongoQsubRunningInfo extends MongoSerializer[QsubRunningInfo, MongoQsubRunningInfo]("QsubRunningInfo", new MongoQsubRunningInfo(_)) {
  def addFields(e: QsubRunningInfo, builder: mutable.Builder[(String, Any), Imports.DBObject]) {
    MongoRunningInfo.addFields(e, builder)
    builder += "jobId" -> e.jobId
    builder += "workingDir" -> e.workingDir.toAbsolute.path
    builder += "outputPathId" -> e.outputPath.id.s
//    builder += "requestedType" -> e.requestedType
  }
}

class MongoQsubRunningInfo(val dbo: MongoDBObject) extends MongoRunningInfo with QsubRunningInfo with MongoWrapper{
  def jobId = dbo.as[Int]("jobId")
  def workingDir = Path.fromString(dbo.as[String]("workingDir"))
  def outputPath = {
    ManagedPath(Identifier[ManagedPath](dbo.as[String]("outputPathId")))
  }
//  def requestedType = dbo.as[String]("requestedType")
}


