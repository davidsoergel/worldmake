package worldmake

import org.joda.time.DateTime
import edu.umass.cs.iesl.scalacommons.Tap._
import scalax.file.Path
import worldmake.storage.{Storage, Identifier}
import scala.collection.{GenSet, GenMap}
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Provenance[+T] {
  def provenanceId: Identifier[Provenance[T]]

  def derivationId: Identifier[Derivation[T]]

  //def output: Option[Artifact[T]]

  //def status: ProvenanceStatus.ProvenanceStatus
  //def setStatus(st:ProvenanceStatus.ProvenanceStatus)

  override def equals(other: Any): Boolean = other match {
    case that: Provenance[T] => (that canEqual this) && provenanceId == that.provenanceId
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Provenance[T]]

  override def hashCode: Int = (41 + provenanceId.hashCode)
}

trait Successful[+T] extends Provenance[T] {
  def output: Artifact[T]
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
  def createdTime: DateTime
  def derivationId = Identifier[Derivation[T]](provenanceId.s)
}

private class MemoryConstantProvenance[T](val output: Artifact[T]) extends ConstantProvenance[T] {
  def createdTime = DateTime.now
  def provenanceId = Identifier[Provenance[T]](output.constantId.s)
}


sealed trait DerivedProvenance[T] extends Provenance[T]

// enforce lifecycle state machine with types

trait BlockedProvenance[T] extends DerivedProvenance[T] {
  def createdTime: DateTime

  def pending(derivedFromUnnamed: GenSet[Successful[_]],derivedFromNamed: GenMap[String, Successful[_]]): PendingProvenance[T] = MemoryPendingProvenance(provenanceId, derivationId, derivedFromUnnamed, derivedFromNamed, createdTime) tap Storage.provenanceStore.put
}

// aka Enqueued
trait PendingProvenance[T] extends DerivedProvenance[T] {
  def createdTime: DateTime

  def enqueuedTime: DateTime

  def derivedFromUnnamed: GenSet[Successful[_]]

  def derivedFromNamed: GenMap[String, Successful[_]]

  def running(runningInfo: RunningInfo): RunningProvenance[T] = MemoryRunningProvenance(provenanceId, derivationId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, DateTime.now(), runningInfo) tap Storage.provenanceStore.put
}

trait RunningProvenance[T] extends DerivedProvenance[T] {
  def createdTime: DateTime

  def enqueuedTime: DateTime

  def derivedFromUnnamed: GenSet[Successful[_]]

  def derivedFromNamed: GenMap[String, Successful[_]]

  def startTime: DateTime

  def runningInfo: RunningInfo

  def failed(exitCode: Int, log: Option[ReadableStringOrFile], cost: Map[CostType.CostType, Double]): FailedProvenance[T] =
    MemoryFailedProvenance(provenanceId, derivationId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost) tap Storage.provenanceStore.put

  def cancelled(exitCode: Int, log: Option[ReadableStringOrFile], cost: Map[CostType.CostType, Double]): CancelledProvenance[T] =
    MemoryCancelledProvenance(provenanceId, derivationId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost) tap Storage.provenanceStore.put

  def completed(exitCode: Int, log: Option[ReadableStringOrFile], cost: Map[CostType.CostType, Double], output: Artifact[T]): CompletedProvenance[T] =
    MemoryCompletedProvenance(provenanceId, derivationId, derivedFromUnnamed, derivedFromNamed, createdTime, enqueuedTime, startTime, runningInfo, DateTime.now(), exitCode, log, cost, output) tap Storage.provenanceStore.put
}

trait PostRunProvenance[T] extends DerivedProvenance[T] {
  def createdTime: DateTime

  def enqueuedTime: DateTime

  def derivedFromUnnamed: GenSet[Successful[_]]

  def derivedFromNamed: GenMap[String, Successful[_]]
  
  def startTime: DateTime
  
  def runningInfo: RunningInfo
  
  def endTime: DateTime

  def exitCode: Int

  def log: Option[ReadableStringOrFile]

  def logString: String = log.map(_.get.fold(x => x, y => y.toString)).getOrElse("")

  def cost: Map[CostType.CostType, Double]
}

trait FailedProvenance[T] extends PostRunProvenance[T]

trait CancelledProvenance[T] extends PostRunProvenance[T]

trait CompletedProvenance[T] extends PostRunProvenance[T] with Successful[T]


object BlockedProvenance {
  // calling this repeatedly with the same ID just overwrites the DB record
  def apply[T](provenanceId: Identifier[Provenance[T]],
               derivationId: Identifier[Derivation[T]],
               enqueueTime: DateTime = DateTime.now()
                ): BlockedProvenance[T] = {
    new MemoryBlockedProvenance[T](provenanceId, derivationId, enqueueTime) tap Storage.provenanceStore.put
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


trait FilenameGenerator {
  def newPath: Path
}

trait ReadableStringOrFile {
  def get: Either[String, Path]
}

/**
 * Accumulate a String up to a given length, then fall back to writing a file when it gets too long.
 * Note a standard/small terminal window (80 x 25) holds 2000 characters total, so a default of 1000 chars seems reasonable
 * @param maxStringLength
 */

class LocalWriteableStringOrFile(fg: FilenameGenerator, maxStringLength: Int = 1000) extends ReadableStringOrFile with Logging {
  implicit val codec = scalax.io.Codec.UTF8
  var current: Either[StringBuffer, Path] = Left(new StringBuffer())

  def write(s: String) {
    current.fold(
      sb => {
        sb.append(s)
        logger.trace(s)
        if (sb.length() > maxStringLength) {
          val p = fg.newPath
          logger.trace("Switching to file store: " + p)
          p.append(sb.toString)
          current = Right(p)
        }
      },
      p => p.write(s))
  }

  def get: Either[String, Path] = current.fold(sb => Left(sb.toString), p => Right(p))

  //override def toString : String = current.fold(sb=>sb.toString,p=>p.toAbsolute.path)
  def getString: String = current.fold(sb => sb.toString, y => y.toString)
}


case class MemoryBlockedProvenance[T](provenanceId: Identifier[Provenance[T]],
                                      derivationId: Identifier[Derivation[T]],
                                      createdTime: DateTime = DateTime.now()
                                       ) extends BlockedProvenance[T]

case class MemoryPendingProvenance[T](provenanceId: Identifier[Provenance[T]],
                                      derivationId: Identifier[Derivation[T]],
                                      derivedFromUnnamed: GenSet[Successful[_]],
                                      derivedFromNamed: GenMap[String, Successful[_]],
                                      createdTime: DateTime,
                                      enqueuedTime: DateTime = DateTime.now()
                                       ) extends PendingProvenance[T]

case class MemoryRunningProvenance[T](provenanceId: Identifier[Provenance[T]],
                                      derivationId: Identifier[Derivation[T]], 
                                      derivedFromUnnamed: GenSet[Successful[_]],
                                      derivedFromNamed: GenMap[String, Successful[_]],
                                      createdTime: DateTime,
                                      enqueuedTime: DateTime,
                                      startTime: DateTime,
                                      runningInfo: RunningInfo) extends RunningProvenance[T]


case class MemoryFailedProvenance[T](provenanceId: Identifier[Provenance[T]],
                                     derivationId: Identifier[Derivation[T]],
                                     derivedFromUnnamed: GenSet[Successful[_]],
                                     derivedFromNamed: GenMap[String, Successful[_]],
                                     createdTime: DateTime,
                                     enqueuedTime: DateTime,
                                     startTime: DateTime,
                                     runningInfo: RunningInfo,
                                     endTime: DateTime,
                                     exitCode: Int,
                                     log: Option[ReadableStringOrFile],
                                     cost: Map[CostType.CostType, Double]
                                      ) extends FailedProvenance[T]

case class MemoryCancelledProvenance[T](provenanceId: Identifier[Provenance[T]],
                                        derivationId: Identifier[Derivation[T]],
                                        derivedFromUnnamed: GenSet[Successful[_]],
                                        derivedFromNamed: GenMap[String, Successful[_]],
                                        createdTime: DateTime,
                                        enqueuedTime: DateTime,
                                        startTime: DateTime,
                                        runningInfo: RunningInfo,
                                        endTime: DateTime,
                                        exitCode: Int,
                                        log: Option[ReadableStringOrFile],
                                        cost: Map[CostType.CostType, Double]
                                         ) extends CancelledProvenance[T]


case class MemoryCompletedProvenance[T](provenanceId: Identifier[Provenance[T]],
                                        derivationId: Identifier[Derivation[T]], 
                                        derivedFromUnnamed: GenSet[Successful[_]],
                                        derivedFromNamed: GenMap[String, Successful[_]],
                                        createdTime: DateTime,
                                        enqueuedTime: DateTime,
                                        startTime: DateTime,
                                        runningInfo: RunningInfo,
                                        endTime: DateTime,
                                        exitCode: Int,
                                        log: Option[ReadableStringOrFile],
                                        cost: Map[CostType.CostType, Double],
                                        output: Artifact[T]
                                         ) extends CompletedProvenance[T]

trait RunningInfo {
  def node: Option[String]

  //def processId: Int
}

trait WithinJvmRunningInfo extends RunningInfo

class MemoryWithinJvmRunningInfo extends WithinJvmRunningInfo {
  //def node = "ThisJVM"
  //def processId = -1
  val node = Some(java.net.InetAddress.getLocalHost.getHostName + " (in JVM)")
}

trait LocalRunningInfo extends RunningInfo {
  def workingDir:Path
}

class MemoryLocalRunningInfo(val workingDir:Path) extends LocalRunningInfo {
  val node = Some(java.net.InetAddress.getLocalHost.getHostName + " (local)")
}
