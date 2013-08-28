package worldmake.storage

import scala.collection.GenSet
import worldmake.{Derivation, Provenance}

trait ProvenanceStore {
  def get[T](id: Identifier[Provenance[T]]): Option[Provenance[T]]

  //def getForArtifact[T](id: Identifier[Artifact[T]]): Option[Provenance[T]]
  def getDerivedFrom[T](id: Identifier[Derivation[T]]): Set[Provenance[T]]

  def getContentHash[T](id: Identifier[Provenance[T]]): Option[String]

  def verifyContentHash[T](id: Identifier[Provenance[T]]) {
    get(id).map(a => assert(a.output.exists(_.contentHash == getContentHash(id))))
  }

  def put[T](provenance: Provenance[T]): Provenance[T]

  //def putConstant[T](artifact: ConstantArtifact[T]): Provenance[T]
}
/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

case class Identifier[+T](s:String) {
  
  // see also FileStore.dirStructure
  val shortM = """(...)(.....)(.*)""".r
  
  val shortM(short1,short2,remainder) = s
  val short = s"$short1/$short2"

  override def toString = s
}
//case class ArtifactIdentifier(s:String)


/*
trait ArtifactStore {
  def get[T](id: Identifier[Artifact[T]]): Set[Artifact[T]]

  def getContentHash[T](id: Identifier[Artifact[T]]): Option[String]

  def verifyContentHash[T](id: Identifier[Artifact[T]]) {
    get(id).map(a => assert(a.contentHash == getContentHash(id)))
  }

  def put[T](artifact: Artifact[T]): Artifact[T]
}

class AggregateArtifactStore(primaryStore: ArtifactStore, otherStores: GenSet[ArtifactStore]) extends ArtifactStore {
  def get[T](id: Identifier[Artifact[T]]) = primaryStore.get(id) ++ otherStores.flatMap(_.get(id)) // perf

  def getContentHash[T](id: Identifier[Artifact[T]]) = primaryStore.getContentHash(id).orElse(otherStores.flatMap(_.getContentHash(id)).headOption) // perf

  def put[T](artifact: Artifact[T]): Artifact[T] = {
    primaryStore.put(artifact)
  }
}
*/



class AggregateProvenanceStore(primaryStore: ProvenanceStore, otherStores: GenSet[ProvenanceStore]) extends ProvenanceStore {

  def get[T](id: Identifier[Provenance[T]]): Option[Provenance[T]] = primaryStore.get(id).orElse(otherStores.flatMap(_.get(id)).headOption) // perf
  
  def getDerivedFrom[T](id: Identifier[Derivation[T]]) = primaryStore.getDerivedFrom(id) ++ otherStores.flatMap(_.getDerivedFrom(id)) // perf

  def getContentHash[T](id: Identifier[Provenance[T]]) = primaryStore.getContentHash(id).orElse(otherStores.flatMap(_.getContentHash(id)).headOption) // perf

  def put[T](provenance: Provenance[T]): Provenance[T] = {
    primaryStore.put(provenance)
  }
}
