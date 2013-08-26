package worldmake

import org.joda.time.DateTime
import edu.umass.cs.iesl.scalacommons.Tap._
import scalax.file.Path
import worldmake.storage.{Storage, Identifier}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait Provenance[T] {
  def provenanceId: Identifier[Provenance[T]]

  def output: Option[Artifact[T]]

  def status: ProvenanceStatus.ProvenanceStatus
}

trait Successful[T] extends Provenance[T] {
  def artifact: Artifact[T] = output.get
}


object ConstantProvenance {
  // this would be nice:
  // if the provenance is not already in the DB, store it with the current timestamp.
  // if it is already there, return the Mongo version with its original timestamp.

  // but for now we keep the memory one with the new timestamp, because the typing is easier

  def apply[T](artifact: Artifact[T]): ConstantProvenance[T] = new MemoryConstantProvenance(artifact) tap Storage.provenanceStore.put

}

// aka Input
trait ConstantProvenance[T] extends Provenance[T] with Successful[T] {
  def createdTime: DateTime

  def output: Some[Artifact[T]]

  def status: ProvenanceStatus.ProvenanceStatus = ProvenanceStatus.Constant
}

private class MemoryConstantProvenance[T](artifact: Artifact[T]) extends ConstantProvenance[T] {
  def createdTime = DateTime.now

  def output = Some(artifact)

  def provenanceId = Identifier[Provenance[T]](artifact.constantId.s)
}


trait DerivedProvenance[T] extends Provenance[T] {

  //def derivation: Derivation[T]

  def derivationId: Identifier[Derivation[T]]

  def derivedFromUnnamed: Set[Provenance[_]]

  def derivedFromNamed: Map[String, Provenance[_]]

  def startTime: DateTime

  def endTime: DateTime

  def exitCode: Option[Integer]

  def log: Option[ReadableStringOrFile]

  def cost: Map[CostType.CostType, Double]

  // todo: store info about cost / duration / etc.
}

object Provenance {
  // calling this repeatedly with the same ID just overwrites the DB record
  def apply[T](provenanceId: Identifier[Provenance[T]],
               derivationId: Identifier[Derivation[T]],
               status: ProvenanceStatus.ProvenanceStatus,
               derivedFromUnnamed: Set[Provenance[_]] = Set.empty,
               derivedFromNamed: Map[String, Provenance[_]] = Map.empty,
               startTime: DateTime = DateTime.now(), endTime: DateTime = DateTime.now(),
               statusCode: Option[Integer] = None,
               log: Option[ReadableStringOrFile] = None,
               output: Option[Artifact[T]] = None,
               cost: Map[CostType.CostType, Double] = Map.empty): Provenance[T] = {
    new MemoryProvenance[T](provenanceId, derivationId, status, derivedFromUnnamed, derivedFromNamed, startTime, endTime, statusCode, log, output, cost) tap Storage.provenanceStore.put
  }
}

object SuccessfulProvenance {
  // calling this repeatedly with the same ID just overwrites the DB record
  def apply[T](provenanceId: Identifier[Provenance[T]],
               derivationId: Identifier[Derivation[T]],
               status: ProvenanceStatus.ProvenanceStatus,
               derivedFromUnnamed: Set[Provenance[_]] = Set.empty,
               derivedFromNamed: Map[String, Provenance[_]] = Map.empty,
               startTime: DateTime = DateTime.now(), endTime: DateTime = DateTime.now(),
               statusCode: Option[Integer] = None,
               log: Option[ReadableStringOrFile] = None,
               output: Option[Artifact[T]] = None,
               cost: Map[CostType.CostType, Double] = Map.empty): Provenance[T] with Successful[T] = { 
    assert(status == ProvenanceStatus.Success)
    new MemoryProvenance[T](provenanceId, derivationId, status, derivedFromUnnamed, derivedFromNamed, startTime, endTime, statusCode, log, output, cost) with Successful[T] tap Storage.provenanceStore.put
  }
  
}

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

object ProvenanceStatus extends Enumeration {
  type ProvenanceStatus = Value
  
  val  Constant, Blocked, Ready, Pending,Running, Cancelled, Success, Failure = Value
}


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

class LocalWriteableStringOrFile(fg: FilenameGenerator, maxStringLength: Int = 1000) extends ReadableStringOrFile {
  implicit val codec = scalax.io.Codec.UTF8
  var current: Either[StringBuffer, Path] = Left(new StringBuffer())

  def write(s: String) {
    current.fold(
      sb => {
        sb.append(s)
        if (sb.length() > maxStringLength) {
          val p = fg.newPath
          p.append(sb.toString)
          current = Right(p)
        }
      },
      p => p.write(s))
  }

  def get: Either[String, Path] = current.fold(sb => Left(sb.toString), p => Right(p))

  //override def toString : String = current.fold(sb=>sb.toString,p=>p.toAbsolute.path)
}


case class MemoryProvenance[T](provenanceId: Identifier[Provenance[T]],
                               derivationId: Identifier[Derivation[T]],
                               status: ProvenanceStatus.ProvenanceStatus,
                               derivedFromUnnamed: Set[Provenance[_]] = Set.empty,
                               derivedFromNamed: Map[String, Provenance[_]] = Map.empty,
                               startTime: DateTime = DateTime.now(), endTime: DateTime = DateTime.now(),
                               exitCode: Option[Integer] = None,
                               log: Option[ReadableStringOrFile] = None,
                               output: Option[Artifact[T]] = None,
                               cost: Map[CostType.CostType, Double] = Map.empty) extends DerivedProvenance[T]
