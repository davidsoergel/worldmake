package worldmake.storage.casbah

import com.mongodb.casbah.Imports._
import worldmake._
import worldmake.storage.{Identifier, ProvenanceStore}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class CasbahProvenanceStore(conn: MongoConnection,
                          dbname: String,
                          collname: String
                           ) extends ProvenanceStore {

  import CasbahProvenanceStore._

  val mongoColl = conn(dbname)(collname)

  override def put[T](provenance: Provenance[T]): Provenance[T] = {
    val existing : Option[Provenance[T]] = mongoColl.findOne(MongoDBObject("_id" -> provenance.provenanceId)).map(provenanceFromDb(_))
    val x : Provenance[T] = existing.getOrElse({
      val result = provenanceToDb[T](provenance).dbo
      mongoColl += result
      provenance
    })
    x
  }

  override def get[T](id: Identifier[Provenance[T]]): Option[Provenance[T]] = {
    val r = mongoColl.findOne(MongoDBObject("_id" -> id.s))
    r.map(provenanceFromDb[T](_))
  }


  def getSuccessful[T](id: Identifier[Successful[T]]) = get(id).map(_.asInstanceOf[Successful[T]])

  override def getDerivedFrom[T](id: Identifier[Derivation[T]]): Set[Provenance[T]] = {
    val r = mongoColl.find(MongoDBObject("derivationId" -> id.s))
    r.map(provenanceFromDb[T](_)).toSet
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
