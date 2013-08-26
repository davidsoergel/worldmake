package worldmake.storage.casbah

import com.mongodb.casbah.Imports._
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.mutable
import com.mongodb.casbah.commons.Imports
import worldmake._
import edu.umass.cs.iesl.scalacommons.util.Hash
import scalax.file.Path
import org.joda.time.DateTime
import worldmake.storage.{StorageContext, ProvenanceStore}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


class CasbahStorage(
                     conn: MongoConnection,
                     dbname: String

                     ) extends StorageContext with Logging {

  // just make sure the initialization stuff loads
  val s = SerializationHelpers
  
  //override val artifactStore: ArtifactStore = new CasbahArtifactStore(conn, dbname, "artifact")
  override val provenanceStore: ProvenanceStore = new CasbahProvenanceStore(conn, dbname, "provenance")
}
/*
class CasbahArtifactStore(conn: MongoConnection,
                          dbname: String,
                          collname: String
                           ) extends ArtifactStore {

  import CasbahArtifactStore._

  val mongoColl = conn(dbname)(collname)

  override def put(artifact: Artifact[_]): Artifact[_] = {
    mongoColl.findOne(MongoDBObject("_id" -> artifact.artifactId)).map(artifactFromDb(_)).getOrElse({
      val result = artifactToDb(artifact).dbo
      mongoColl += result
      artifact
    })
  }

  override def get(id: String): Option[Artifact[_]] = {
    val r = mongoColl.findOne(MongoDBObject("_id" -> id))
    r.map(artifactFromDb(_))
  }

  def getContentHash(id: String) = get(id).map(_.contentHash)
}

*/










