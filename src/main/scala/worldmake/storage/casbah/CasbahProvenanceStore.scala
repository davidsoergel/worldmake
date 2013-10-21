package worldmake.storage.casbah

import com.mongodb.casbah.Imports._
import worldmake._
import worldmake.storage.{ProvenanceStoreStatus, Identifier, ProvenanceStore}

import com.mongodb.casbah.query.Imports.IntOk
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class CasbahProvenanceStore(conn: MongoConnection,
                          dbname: String,
                          collname: String
                           ) extends ProvenanceStore with Logging {

  import CasbahProvenanceStore._

  val mongoColl = conn(dbname)(collname)
  
  mongoColl.ensureIndex("recipeId")

  override def put[T](provenance: Provenance[T]) {
    val existing : Option[Provenance[T]] = mongoColl.findOne(MongoDBObject("_id" -> provenance.provenanceId)).map(provenanceFromDb(_))
    //val x : Provenance[T] = existing.getOrElse({
      val result = provenanceToDb[T](provenance).dbo
      mongoColl += result
    
      existing match {
        case x: MongoDependenciesBoundProvenance[_] => { }
        case y => {
          // dependencies were not already bound, so increment their ref counts
          provenance match {
            case d: MongoDependenciesBoundProvenance[_] => ( d.derivedFromUnnamed ++ d.derivedFromNamed.values).map(x=>incrementRefCount(x.provenanceId))
            case _ => 
          }
        }
      }
     
      //provenance
    //})
    //x
  }

  def incrementRefCount(provenanceId: Identifier[Provenance[_]]) {
    mongoColl.update(MongoDBObject("_id" -> provenanceId), $inc("refcount" -> 1), false, false)
  }
  
  def decrementRefCount(provenanceId: Identifier[Provenance[_]]) {
    // todo assert refcount >= 0 afterwards
    mongoColl.update(MongoDBObject("_id" -> provenanceId), $inc("refcount" -> -1), false, false)
  }
  def protect(provenanceId: Identifier[Provenance[_]]) {
    mongoColl.update(MongoDBObject("_id" -> provenanceId), $set(Seq("protect" -> 1)), false, false)
  }
  def unprotect(provenanceId: Identifier[Provenance[_]]){
    mongoColl.update(MongoDBObject("_id" -> provenanceId), $unset(Seq("protect")), false, false)
  }

  override def get[T](id: Identifier[Provenance[T]]): Option[Provenance[T]] = {
    val r = mongoColl.findOne(MongoDBObject("_id" -> id.s))
    r.map(provenanceFromDb[T](_))
  }


  override def findWithArtifactByValue(s: String) = {
    // todo don't just assume that the artifact is stored as toString
    val r = mongoColl.find(MongoDBObject("output.value" -> s))
    r.map(provenanceFromDb[Any](_)).toSet
  }

  override def findWithArtifactFileByIdFragment(s: String) = {
    val frag = s.replaceAll("/","")
    val r = mongoColl.find(MongoDBObject("output.fileId" -> s".*$frag.*".r))
    r.map(provenanceFromDb[Any](_)).toSet
  }
  
  def findWithLogFileById(s: Identifier[ManagedPath]) =  {
    val r = mongoColl.find(MongoDBObject("log.fileId" -> s))
    r.map(provenanceFromDb[Any](_)).toSet
  }

  def findWithArtifactFileById(s: Identifier[ManagedPath]) =  {
    val r = mongoColl.find(MongoDBObject("output.fileId" -> s))
    r.map(provenanceFromDb[Any](_)).toSet
  }


  def status = {
    val blocked = mongoColl.count(MongoDBObject("type" -> MongoBlockedProvenance.typehint)).toInt
    val pending = mongoColl.count(MongoDBObject("type" -> MongoPendingProvenance.typehint)).toInt
    val successful = mongoColl.count(MongoDBObject("type" -> MongoCompletedProvenance.typehint)).toInt
    val failed = mongoColl.count(MongoDBObject("type" -> MongoFailedProvenance.typehint)).toInt
    val cancelled = mongoColl.count(MongoDBObject("type" -> MongoCancelledProvenance.typehint)).toInt
    new ProvenanceStoreStatus(blocked, pending, successful, failed, cancelled)
  }

  override def removeUnused : Boolean = {
    val g = mongoColl.find(MongoDBObject("refcount" -> 0))
    for(dbo <- g){
      val p = provenanceFromDb(dbo)
      p match { case d: MongoDependenciesBoundProvenance[_] => ( d.derivedFromUnnamed ++ d.derivedFromNamed.values).map(x=>decrementRefCount(x.provenanceId)) }
      // slow, but there's no easy bulk remove?
      mongoColl.remove(dbo)
    }
    g.nonEmpty     
  }
  
  override def removeDead = {
    mongoColl.remove(MongoDBObject("type" -> MongoFailedProvenance.typehint))
    mongoColl.remove(MongoDBObject("type" -> MongoCancelledProvenance.typehint))
  }

  override def removeZombie = {
    mongoColl.remove(MongoDBObject("type" -> MongoRunningProvenance.typehint))
    mongoColl.remove(MongoDBObject("type" -> MongoPendingProvenance.typehint))
    mongoColl.remove(MongoDBObject("type" -> MongoBlockedProvenance.typehint))
  }

  def getSuccessful[T](id: Identifier[Successful[T]]) = get(id).map(_.asInstanceOf[MongoSuccessful[T]])

  override def getDerivedFrom[T](id: Identifier[Recipe[T]]): Set[Provenance[T]] = {
    logger.info(s"Looking for provenances derived from recipe ${id.s}" )
    val r = mongoColl.find(MongoDBObject("recipeId" -> id.s))
    val result = r.map(provenanceFromDb[T](_)).toSet
    logger.info(s"Found ${result.size} provenances for recipe ${id.s}")
    result
  }

  def getContentHash[T](id: Identifier[Provenance[T]]): Option[String] = get(id).map({
    case d:Successful[_] => d.output
  }).map(_.contentHash)
}


object CasbahProvenanceStore {

  import scalaz.syntax.std.option._

  def provenanceFromDb[T](dbo: MongoDBObject): Provenance[T] =
    provenanceFromDbOpt[T](dbo).getOrElse(sys.error("cannot deserialize object of type '" + dbo("type") + "'"))

  def provenanceFromDbOpt[T](dbo: MongoDBObject): Option[Provenance[T]] = {
    dbo("type") match {
      case MongoConstantProvenance.typehint => new MongoConstantProvenance[T](dbo).some
      case MongoBlockedProvenance.typehint => new MongoBlockedProvenance[T](dbo).some
      case MongoPendingProvenance.typehint => new MongoPendingProvenance[T](dbo).some
      case MongoRunningProvenance.typehint => new MongoRunningProvenance[T](dbo).some
      case MongoFailedProvenance.typehint => new MongoFailedProvenance[T](dbo).some
      case MongoCancelledProvenance.typehint => new MongoCancelledProvenance[T](dbo).some
      case MongoCompletedProvenance.typehint => new MongoCompletedProvenance[T](dbo).some
        
      case _ => None
    }
  }

  def provenanceToDb[T](e: Provenance[T]): MongoWrapper = e match {
    case e: ConstantProvenance[T] => MongoConstantProvenance.toDb(e)
    case e: BlockedProvenance[T] => MongoBlockedProvenance.toDb(e)
    case e: PendingProvenance[T] => MongoPendingProvenance.toDb(e)
    case e: RunningProvenance[T] => MongoRunningProvenance.toDb(e)
    case e: FailedProvenance[T] => MongoFailedProvenance.toDb(e)
    case e: CancelledProvenance[T] => MongoCancelledProvenance.toDb(e)
    case e: CompletedProvenance[T] => MongoCompletedProvenance.toDb(e)
  }
}
