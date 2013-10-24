package worldmake

import org.joda.time.DateTime
import edu.umass.cs.iesl.scalacommons.Tap._
import scalax.file.Path
import worldmake.storage.{Storage, Identifier}
import scala.collection.{mutable, GenSet, GenMap}
import com.typesafe.scalalogging.slf4j.Logging
import java.io.PrintStream
import edu.umass.cs.iesl.scalacommons.IOUtils
import scalax.io.{Output, Resource}
import scala.io.Source
import edu.umass.cs.iesl.scalacommons.collections.FiniteMutableQueue
import scala.collection.immutable.Queue
import scalax.file.defaultfs.DefaultPath

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Provenance[+T] extends WorldmakeEntity {
  def createdTime: DateTime

  def modifiedTime: DateTime = createdTime
  
  def provenanceId: Identifier[Provenance[T]]

  def recipeId: Identifier[Recipe[T]]

  //def output: Option[Artifact[T]]

  //def status: ProvenanceStatus.ProvenanceStatus
  //def setStatus(st:ProvenanceStatus.ProvenanceStatus)

  override def equals(other: Any): Boolean = other match {
    case that: Provenance[T] => (that canEqual this) && provenanceId == that.provenanceId
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Provenance[T]]

  override lazy val hashCode: Int = (41 + provenanceId.hashCode)

  lazy val queue: Queue[Provenance[_]] = Queue(this)
  
  def status : String
  def statusPairs : Seq[(String, Any)] = Seq("Provenance" -> provenanceId, "Created" -> createdTime)
  
  def statusBlock = statusPairs.map({case (k,v) => f"$k%16s: $v%64s"}).mkString("\n")
  
  def statusLine : String = f"${provenanceId.short}%10s $status%10s ${modifiedTime}%10s ${recipeId.short}%10s "
  
  
}

trait Successful[+T] extends PostRunProvenance[T] {
  def output: Artifact[T]
  override lazy val statusPairs = super.statusPairs :+ ("Value"->output)
  override def statusLine : String = super.statusLine + f" ${output}%10s"
  
  
}


object ConstantProvenance {
  // this would be nice:
  // if the provenance is not already in the DB, store it with the current timestamp.
  // if it is already there, return the Mongo version with its original timestamp.

  // but for now we keep the memory one with the new timestamp, because the typing is easier

  def apply[T](artifact: Artifact[T]): ConstantProvenance[T] = new MemoryConstantProvenance(artifact) tap Storage.provenanceStore.put

}

// aka Input
trait ConstantProvenance[T] extends Successful[T] {
  //def createdTime: DateTime
  lazy val recipeId = Identifier[Recipe[T]](provenanceId.s)
  val status = "Constant"
  
  def derivedFromNamed = Map.empty

  def derivedFromUnnamed = Set.empty

  def cost = Map.empty

  def endTime = createdTime

  def enqueuedTime = createdTime

  def exitCode = 0

  def log = None

  def runningInfo = ConstantRunningInfo

  def startTime = createdTime
}

private class MemoryConstantProvenance[T](val output: Artifact[T]) extends ConstantProvenance[T] {
  val createdTime = DateTime.now
  lazy val provenanceId = Identifier[Provenance[T]](output.constantId.s)

}


sealed trait DerivedProvenance[+T] extends Provenance[T] {

  // Set is not covariant?!?  OK, just use Seq instead for now
  //def derivedFromAll : Seq[Provenance[_]]
/*
  override lazy val queue: Queue[Provenance[_]] = {
    val deps = derivedFromAll.flatMap(_.queue)
    Queue[Provenance[_]](deps: _*).distinct.enqueue(this)
  }*/
}

// enforce lifecycle state machine with types

trait BlockedProvenance[T] extends DerivedProvenance[T] {
  //def createdTime: DateTime
  val status = "Blocked"

  //def derivedFromUnnamed: GenSet[Provenance[_]]

  //def derivedFromNamed: GenMap[String, Provenance[_]]
  //override def derivedFromAll : Seq[Provenance[_]] = (derivedFromUnnamed ++ derivedFromNamed.values).seq.toSeq

  def pending(derivedFromUnnamed: GenSet[Successful[_]],derivedFromNamed: GenMap[String, Successful[_]]): PendingProvenance[T] = MemoryPendingProvenance(provenanceId, recipeId, derivedFromUnnamed, derivedFromNamed, createdTime) tap Storage.provenanceStore.put
}

sealed trait DependenciesBoundProvenance[+T] extends DerivedProvenance[T] {
   def derivedFromUnnamed: GenSet[Successful[_]]

   def derivedFromNamed: GenMap[String, Successful[_]]

  //override 
  def derivedFromAll : Seq[Provenance[_]] = (derivedFromUnnamed ++ derivedFromNamed.values).seq.toSeq


}

// aka Enqueued
trait PendingProvenance[T] extends DependenciesBoundProvenance[T]  {
  //def createdTime: DateTime

  val status = "Pending"
  def enqueuedTime: DateTime

  override def modifiedTime: DateTime = enqueuedTime


  def running(runningInfo: RunningInfo): RunningProvenance[T] = MemoryRunningProvenance(provenanceId, recipeId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, DateTime.now(), runningInfo) tap Storage.provenanceStore.put
}

trait RunningProvenance[T] extends DependenciesBoundProvenance[T]  {
  //def createdTime: DateTime

  val status = "Running"
  def enqueuedTime: DateTime

  def startTime: DateTime

  override def modifiedTime: DateTime = startTime

  def runningInfo: RunningInfo

  override def statusPairs = super.statusPairs ++ runningInfo.statusPairs

  def failed(exitCode: Int, log: Option[ReadableStringOrManagedFile], cost: Map[CostType.CostType, Double]): FailedProvenance[T] =
    MemoryFailedProvenance(provenanceId, recipeId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost) tap Storage.provenanceStore.put

  def cancelled(exitCode: Int, log: Option[ReadableStringOrManagedFile], cost: Map[CostType.CostType, Double]): CancelledProvenance[T] =
    MemoryCancelledProvenance(provenanceId, recipeId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost) tap Storage.provenanceStore.put

  def completed(exitCode: Int, log: Option[ReadableStringOrManagedFile], cost: Map[CostType.CostType, Double], output: Artifact[T]): CompletedProvenance[T] =
    MemoryCompletedProvenance(provenanceId, recipeId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost, output) tap Storage.provenanceStore.put
}

trait PostRunProvenance[+T] extends DependenciesBoundProvenance[T]  {
  //def createdTime: DateTime
  

  def enqueuedTime: DateTime
  
  def startTime: DateTime
  
  def runningInfo: RunningInfo
  
  def endTime: DateTime

  override def modifiedTime: DateTime = endTime

  def exitCode: Int

  def log: Option[ReadableStringOrManagedFile]

  def logString: String = log.map(_.get.fold(x => x, y => y.toString)).getOrElse("")
  
  def printLog(os:Output) = log.map(_.get.fold(s=>os.write(s),p=>p.path.copyDataTo(os)))
  
  import FiniteMutableQueue._
  private def headTail(lines:Iterator[String], h:Int,t:Int) : String = {
    //val lines = s.getLines
    val hLines = lines.take(h)
    val tLines = {
      val q = mutable.Queue[String]()
      q.enqueueFinite[String](lines,t)
      q.toIterator
    }
    hLines.mkString("\n") + s"\n\n[...\nskipped middle\n...]\n\n" + tLines.mkString("\n")
    
  }
  def printLogHeadTail(os:Output) = log.map(_.get.fold(s=>os.write(headTail(s.split("\n").toIterator,100,100)),p=>os.write(headTail(Source.fromFile(p.path.fileOption.get).getLines(),100,100))))

  def cost: Map[CostType.CostType, Double]

  override def statusPairs = (super.statusPairs :+ "Start"->startTime :+ "End" -> endTime) ++ runningInfo.statusPairs
 
}

trait FailedProvenance[T] extends PostRunProvenance[T] {
  override def statusPairs = super.statusPairs :+ "Log"->logString

  val status = "Failed"
}

trait CancelledProvenance[T] extends PostRunProvenance[T] {

  val status = "Cancelled"
}

trait CompletedProvenance[T] extends PostRunProvenance[T] with Successful[T] {

  val status = "Success"
}

object InstantCompletedProvenance {
  def apply[T](provenanceId: Identifier[Provenance[T]],
             recipeId: Identifier[Recipe[T]],
             derivedFromUnnamed: GenSet[Successful[_]],
             derivedFromNamed: GenMap[String, Successful[_]],
             createdTime: DateTime,
             enqueuedTime: DateTime,
             startTime: DateTime,
             runningInfo: RunningInfo,
             endTime: DateTime,
             exitCode: Int,
             log: Option[ReadableStringOrManagedFile],
             cost: Map[CostType.CostType, Double],
             output: Artifact[T]
    ) : CompletedProvenance[T] =  {
    new MemoryCompletedProvenance(provenanceId, recipeId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost,output) tap Storage.provenanceStore.put
  }
}

object InstantFailedProvenance {
  def apply[T](provenanceId: Identifier[Provenance[T]],
               recipeId: Identifier[Recipe[T]],
               derivedFromUnnamed: GenSet[Successful[_]],
               derivedFromNamed: GenMap[String, Successful[_]],
               createdTime: DateTime,
               enqueuedTime: DateTime,
               startTime: DateTime,
               runningInfo: RunningInfo,
               endTime: DateTime,
               exitCode: Int,
               log: Option[ReadableStringOrManagedFile],
               cost: Map[CostType.CostType, Double]
                ) : FailedProvenance[T] =  {
    new MemoryFailedProvenance(provenanceId, recipeId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost) tap Storage.provenanceStore.put
  }
}

object BlockedProvenance {
  // calling this repeatedly with the same ID just overwrites the DB record
  def apply[T](provenanceId: Identifier[Provenance[T]],
               recipeId: Identifier[Recipe[T]],
               //derivedFromUnnamed: GenSet[Provenance[_]],
               //derivedFromNamed: GenMap[String, Provenance[_]],
               enqueueTime: DateTime = DateTime.now()
                ): BlockedProvenance[T] = {
    new MemoryBlockedProvenance[T](provenanceId, recipeId, //derivedFromUnnamed, derivedFromNamed,
      enqueueTime) tap Storage.provenanceStore.put
  }
}

/*
object SuccessfulProvenance {
  // calling this repeatedly with the same ID just overwrites the DB record
  def apply[T](provenanceId: Identifier[Provenance[T]],
               derivationId: Identifier[Derivation[T]],
               status: ProvenanceStatus.ProvenanceStatus,
               derivedFromUnnamed: GenSet[Successful[_]] = Set.empty,
               derivedFromNamed: GenMap[String, Successful[_]] = Map.empty,
               enqueueTime: DateTime = DateTime.now(),
               startTime: Option[DateTime] = None,
               endTime: Option[DateTime] = None,
               statusCode: Option[Int] = None,
               log: Option[ReadableStringOrFile] = None,
               output: Option[Artifact[T]] = None,
               cost: Map[CostType.CostType, Double] = Map.empty): Successful[T] = { 
    assert(status == ProvenanceStatus.Success)
    new MemoryProvenance[T](provenanceId, derivationId, status, derivedFromUnnamed, derivedFromNamed,enqueueTime, startTime, endTime, statusCode, log, output, cost) with Successful[T] tap Storage.provenanceStore.put
  }
  
}
*/

/*
object ProvenanceStatus {
  def apply(s: String) = s match {
    case Constant.name => Constant
    case Blocked.name => Blocked
    case Ready.name => Ready
    case Pending.name => Pending
    case Running.name => Running
    case Cancelled.name => Cancelled
    case Success.name => Success
    case Failure.name => Failure
  }
}

sealed abstract class ProvenanceStatus(val name: String)

case object Constant extends ProvenanceStatus("Constant")

// prereqs are not
case object Blocked extends ProvenanceStatus("Blocked")

// prereqs are done
case object Ready extends ProvenanceStatus("Ready")

// job has been enqueued
case object Pending extends ProvenanceStatus("Pending")

case object Running extends ProvenanceStatus("Running")

case object Cancelled extends ProvenanceStatus("Cancelled")

case object Success extends ProvenanceStatus("Success")

case object Failure extends ProvenanceStatus("Failure")
*/

// todo replace these with traits / types

/*
object ProvenanceStatus extends Enumeration {
  type ProvenanceStatus = Value
  
  val  Constant, 
  Explicit, 
  Blocked, 
  Ready, 
  Pending, 
  Running, 
  Cancelled, 
  Success,
  Failure = Value
}
*/

object CostType extends Enumeration {
  type CostType = Value

  val WallclockTime,
  CpuUserTime,
  CpuSystemTime,
  CpuIOWaitTime,
  TotalNetworkBytes,
  MaxNetworkReadBandwidth,
  MaxNetworkWriteBandwidth,
  MaxDiskReadBandwidth,
  MaxDiskWriteBandwidth,
  TempDiskSpace,
  MaxMemory,
  USD,
  HumanInteractionTime = Value
}


trait PathReference {
  def path: Path
  def abspath = path.toAbsolute.path
  def child(s:String) : PathReference
}

object ExternalPath {
  def apply(_path:Path) : ExternalPath = new ExternalPath {
    val path = _path
  }
}

trait ExternalPath extends PathReference {

  //todo throw informative errors when underlying files are missing
  // for now just assume the files always resolve
  // def get: Option[Path] = Storage.fileStore.get(id)

  def path: Path
  //def get: Path = path
  def child(s:String) = ExternalPath(path / s)
}


object ManagedPath {
  def apply(_id: Identifier[ManagedPath], rel:Option[Path] = None) : ManagedPath = new ManagedPath {
    require(!_id.s.contains("/"))
    val id = _id
    override val relative = rel
  }
  /*
  val emptyPath = {
    val result = Path(Seq.empty:_*) //Path.fromString("")
    for(s <- result.segments.tail) { assert(!s.contains("/")) }
    result
  }
  */
}

trait ManagedPath extends PathReference {
  
  //todo throw informative errors when underlying files are missing
  // for now just assume the files always resolve
  // def get: Option[Path] = Storage.fileStore.get(id)
  
  def id: Identifier[ManagedPath]
  def relative: Option[Path] = None
  def path: Path = {
    val base: Path = Storage.fileStore.getOrCreate(id)
    val result = relative.map(r=>{
      base / r
    }).getOrElse(base)
    for(s <- result.segments.tail) { assert(!s.contains("/")) }
    result
  }
  def pathLog: Path = {
    val base = Storage.logStore.getOrCreate(id)
    relative.map(r=>base / r).getOrElse(base)
  }
  def abspathLog = pathLog.toAbsolute.path
  def child(s:String) = ManagedPath(id, relative.map(r=>r / s).orElse(Some(Path(s))))
}

trait ManagedFileStore {
  def newId: Identifier[ManagedPath]
  def exists(id: Identifier[ManagedPath]):Boolean
  def get(id: Identifier[ManagedPath]): Option[Path]
  def getOrCreate(id: Identifier[ManagedPath]): Path
}

trait ReadableStringOrManagedFile {
  def get: Either[String, ManagedPath]
}

/**
 * Accumulate a String up to a given length, then fall back to writing a file when it gets too long.
 * Note a standard/small terminal window (80 x 25) holds 2000 characters total, so a default of 1000 chars seems reasonable
 * @param maxStringLength
 */

class LocalWriteableStringOrManagedFile(fg: ManagedFileStore, maxStringLength: Int = 1000) extends ReadableStringOrManagedFile with Logging {
  implicit val codec = scalax.io.Codec.UTF8
  
  
  var current: Either[StringBuffer, Output] = Left(new StringBuffer())
  var count = 0

  def write(s: String) = synchronized {
    current.fold(
      sb => {
        logger.trace("Appending to StringBuffer: " + s)
        sb.append(s)
        logger.trace(s)
        if (sb.length() > maxStringLength) {
          val logId = fg.newId
          val p = fg.getOrCreate(logId)
          logger.trace("Switching to file store: " + p)
          p.append(sb.toString)
          current = Right(ManagedPath(logId).path)
        }
      },
      o => {
        //logger.trace("Appending to Path: " + p)
        o.write(s)
      })
    count += s.length
  }

  def get: Either[String, ManagedPath] = current.fold(sb => Left(sb.toString), p => Right(p))

  //override def toString : String = current.fold(sb=>sb.toString,p=>p.toAbsolute.path)
  def getString: String = current.fold(sb => sb.toString, y => y.toString)
}


case class MemoryBlockedProvenance[T](provenanceId: Identifier[Provenance[T]],
                                      recipeId: Identifier[Recipe[T]],
                                      //derivedFromUnnamed: GenSet[Provenance[_]],
                                      //derivedFromNamed: GenMap[String, Provenance[_]],
                                      createdTime: DateTime = DateTime.now()
                                       ) extends BlockedProvenance[T]

case class MemoryPendingProvenance[T](provenanceId: Identifier[Provenance[T]],
                                      recipeId: Identifier[Recipe[T]],
                                      derivedFromUnnamed: GenSet[Successful[_]],
                                      derivedFromNamed: GenMap[String, Successful[_]],
                                      createdTime: DateTime,
                                      enqueuedTime: DateTime = DateTime.now()
                                       ) extends PendingProvenance[T]

case class MemoryRunningProvenance[T](provenanceId: Identifier[Provenance[T]],
                                      recipeId: Identifier[Recipe[T]], 
                                      derivedFromUnnamed: GenSet[Successful[_]],
                                      derivedFromNamed: GenMap[String, Successful[_]],
                                      createdTime: DateTime,
                                      enqueuedTime: DateTime,
                                      startTime: DateTime,
                                      runningInfo: RunningInfo) extends RunningProvenance[T]


case class MemoryFailedProvenance[T](provenanceId: Identifier[Provenance[T]],
                                     recipeId: Identifier[Recipe[T]],
                                     derivedFromUnnamed: GenSet[Successful[_]],
                                     derivedFromNamed: GenMap[String, Successful[_]],
                                     createdTime: DateTime,
                                     enqueuedTime: DateTime,
                                     startTime: DateTime,
                                     runningInfo: RunningInfo,
                                     endTime: DateTime,
                                     exitCode: Int,
                                     log: Option[ReadableStringOrManagedFile],
                                     cost: Map[CostType.CostType, Double]
                                      ) extends FailedProvenance[T]

case class MemoryCancelledProvenance[T](provenanceId: Identifier[Provenance[T]],
                                        recipeId: Identifier[Recipe[T]],
                                        derivedFromUnnamed: GenSet[Successful[_]],
                                        derivedFromNamed: GenMap[String, Successful[_]],
                                        createdTime: DateTime,
                                        enqueuedTime: DateTime,
                                        startTime: DateTime,
                                        runningInfo: RunningInfo,
                                        endTime: DateTime,
                                        exitCode: Int,
                                        log: Option[ReadableStringOrManagedFile],
                                        cost: Map[CostType.CostType, Double]
                                         ) extends CancelledProvenance[T]


case class MemoryCompletedProvenance[T](provenanceId: Identifier[Provenance[T]],
                                        recipeId: Identifier[Recipe[T]], 
                                        derivedFromUnnamed: GenSet[Successful[_]],
                                        derivedFromNamed: GenMap[String, Successful[_]],
                                        createdTime: DateTime,
                                        enqueuedTime: DateTime,
                                        startTime: DateTime,
                                        runningInfo: RunningInfo,
                                        endTime: DateTime,
                                        exitCode: Int,
                                        log: Option[ReadableStringOrManagedFile],
                                        cost: Map[CostType.CostType, Double],
                                        output: Artifact[T]
                                         ) extends CompletedProvenance[T]

trait RunningInfo {
  def node: Option[String]

  def statusPairs : Seq[(String, Any)] = Seq("Node"->node.getOrElse(""))
  //def processId: Int
}

trait WithinJvmRunningInfo extends RunningInfo {

  override def statusPairs = super.statusPairs :+  "Run"->"within JVM"
}

class MemoryWithinJvmRunningInfo extends WithinJvmRunningInfo {
  //def node = "ThisJVM"
  //def processId = -1
  val node = Some(java.net.InetAddress.getLocalHost.getHostName + " (in JVM)")
}

trait LocalRunningInfo extends RunningInfo {
  def workingDir:Path

  override def statusPairs = super.statusPairs :+  "Working Dir"->workingDir
}

class MemoryLocalRunningInfo(val workingDir:Path) extends LocalRunningInfo {
  val node = Some(java.net.InetAddress.getLocalHost.getHostName + " (local)")
}

object ConstantRunningInfo extends RunningInfo{
  def node = None
}
