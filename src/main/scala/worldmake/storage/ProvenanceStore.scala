package worldmake.storage

import scala.collection.GenSet
import worldmake._
import scalax.file.Path

case class ProvenanceStoreStatus(blocked:Int,pending:Int,successful:Int,failed:Int,cancelled:Int) {
  def +(other:ProvenanceStoreStatus) = ProvenanceStoreStatus(blocked + other.blocked, pending+other.pending,successful+other.successful,failed+other.failed,cancelled+other.cancelled) 
}

trait ProvenanceStore {
  def removeUnused : Boolean

  def removeDead

  def removeZombie

  def get[T](id: Identifier[Provenance[T]]): Option[Provenance[T]]

  //def getWithRecipeId(s: String): Option[Provenance[_]]

  def findWithLogFileById(s: Identifier[ManagedPath]): Set[Provenance[_]]
  def findWithArtifactFileById(s: Identifier[ManagedPath]): Set[Provenance[_]]
  def findWithArtifactFileByIdFragment(s: String): Set[Provenance[_]]
  def findWithArtifactByValue(s: String): Set[Provenance[_]]


  def status : ProvenanceStoreStatus

  def getSuccessful[T](id: Identifier[Successful[T]]): Option[Successful[T]]

  //def getForArtifact[T](id: Identifier[Artifact[T]]): Option[Provenance[T]]
  def getDerivedFrom[T](id: Identifier[Recipe[T]]): Set[Provenance[T]]

  def getContentHash[T](id: Identifier[Provenance[T]]): Option[String]

  def verifyContentHash[T](id: Identifier[Provenance[T]]) {
    get(id).map({
      case aa : Successful[T] => assert (aa.output.contentHash == getContentHash(id))
    })
  }

  def put[T](provenance: Provenance[T]) //: Provenance[T]

  //def putConstant[T](artifact: ConstantArtifact[T]): Provenance[T]
}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

case class Identifier[+T](s: String) {
  require(!s.startsWith("Nothing"))

  // see also FileStore.dirStructure
  val shortM = """(...)(.....)(.*)""".r

  val shortM(short1, short2, remainder) = s
  val short = s"$short1/$short2"

  override def toString = s

  override def equals(other: Any): Boolean = other match {
    case that: Identifier[T] => (that canEqual this) && s == that.s
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Identifier[T]]

  override def hashCode: Int = (41 + s.hashCode)
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
  def getSuccessful[T](id: Identifier[Successful[T]]): Option[Successful[T]] = primaryStore.getSuccessful(id).orElse(otherStores.flatMap(_.getSuccessful(id)).headOption) // perf

  def getDerivedFrom[T](id: Identifier[Recipe[T]]) = primaryStore.getDerivedFrom(id) ++ otherStores.flatMap(_.getDerivedFrom(id)) // perf

  def getContentHash[T](id: Identifier[Provenance[T]]) = primaryStore.getContentHash(id).orElse(otherStores.flatMap(_.getContentHash(id)).headOption) // perf

  def put[T](provenance: Provenance[T]) { //: Provenance[T] = {
    primaryStore.put(provenance)
  }

  def findWithArtifactByValue(s: String) = primaryStore.findWithArtifactByValue(s) ++ otherStores.flatMap(_.findWithArtifactByValue(s))

  def findWithArtifactFileById(s: Identifier[ManagedPath]) = primaryStore.findWithArtifactFileById(s) ++ otherStores.flatMap(_.findWithArtifactFileById(s))

  def findWithArtifactFileByIdFragment(s: String) = primaryStore.findWithArtifactFileByIdFragment(s) ++ otherStores.flatMap(_.findWithArtifactFileByIdFragment(s))

  def findWithLogFileById(s: Identifier[ManagedPath]) = primaryStore.findWithLogFileById(s) ++ otherStores.flatMap(_.findWithLogFileById(s))

  def removeDead = { primaryStore.removeDead; otherStores.map(_.removeDead) }

  def removeUnused = otherStores.map(_.removeUnused).fold(primaryStore.removeUnused)((a,b)=>a|b)  // non-short-circuit

  def removeZombie =  { primaryStore.removeZombie; otherStores.map(_.removeZombie) }

  def status = otherStores.map(_.status).fold(primaryStore.status)((a,b)=>a+b)
}


object StoredProvenancesForRecipe {
  def apply[T](id: Identifier[Recipe[T]]) = new StoredProvenancesForRecipe(id)
}

class StoredProvenancesForRecipe[T](val recipeId: Identifier[Recipe[T]]) {

  val all: GenSet[Provenance[T]] = Storage.provenanceStore.getDerivedFrom(recipeId)

  lazy val successes: GenSet[Successful[T]] =
    all.collect({
      case x: Successful[T] => x
    })

  lazy val blocked: GenSet[BlockedProvenance[T]] =
    all.collect({
      case x: BlockedProvenance[T] => x
    })

  lazy val pending: GenSet[PendingProvenance[T]] =
    all.collect({
      case x: PendingProvenance[T] => x
    })

  lazy val running: GenSet[RunningProvenance[T]] =
    all.collect({
      case x: RunningProvenance[T] => x
    })

  lazy val potentialSuccesses: GenSet[DerivedProvenance[T]] =
    all.collect({
      case x: BlockedProvenance[T] => x
      case x: PendingProvenance[T] => x
      case x: RunningProvenance[T] => x
    })


  lazy val failures: GenSet[FailedProvenance[T]] =
    all.collect({
      case x: FailedProvenance[T] => x
    })

  lazy val cancelled: GenSet[CancelledProvenance[T]] =
    all.collect({
      case x: CancelledProvenance[T] => x
    })


  /** Just report the most interesting result, not all of them
    *
    */
  lazy val statusString: String = {
    if (successes.size > 0) {
      "Success (" + successes.size + ")"
    } else if (running.size > 0) {
      "Running (" + running.size + ")"
    } else if (pending.size > 0) {
      "Pending (" + pending.size + ")"
    } else if (blocked.size > 0) {
      "Blocked (" + blocked.size + ")"
    } else if (cancelled.size > 0) {
      "Cancelled (" + cancelled.size + ")"
    } else if (failures.size > 0) {
      "Failure (" + failures.size + ")"
    }
    else "no status" // todo print other statuses
  }
  
  def mostRecent : Option[Provenance[T]] = if(all.isEmpty) None else Some(all.toSeq.max(new Ordering[Provenance[T]]{
    def compare(x: Provenance[T], y: Provenance[T]) = x.modifiedTime.compareTo(y.modifiedTime)
  }))

}
