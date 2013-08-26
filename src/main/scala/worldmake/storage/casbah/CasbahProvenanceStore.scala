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

  override def getDerivedFrom[T](id: Identifier[Derivation[T]]): Set[Provenance[T]] = {
    val r = mongoColl.find(MongoDBObject("derivationId" -> id.s))
    r.map(provenanceFromDb[T](_)).toSet
  }

  def getContentHash[T](id: Identifier[Provenance[T]]) = get(id).flatMap(_.output).map(_.contentHash)
}


object CasbahProvenanceStore {

  import scalaz.syntax.std.option._

  def provenanceFromDb[T](dbo: MongoDBObject): Provenance[T] =
    provenanceFromDbOpt[T](dbo).getOrElse(sys.error("cannot deserialize object of type '" + dbo("type") + "'"))

  def provenanceFromDbOpt[T](dbo: MongoDBObject): Option[Provenance[T]] = {
    dbo("type") match {
      case MongoConstantProvenance.typehint => new MongoConstantProvenance[T](dbo).some
      case MongoDerivedProvenance.typehint => new MongoDerivedProvenance[T](dbo).some
      case _ => None
    }
  }

  def provenanceToDb[T](e: Provenance[T]): MongoWrapper = e match {
    case e: ConstantProvenance[T] => MongoConstantProvenance.toDb(e)
    case e: DerivedProvenance[T] => MongoDerivedProvenance.toDb(e)  }
}
