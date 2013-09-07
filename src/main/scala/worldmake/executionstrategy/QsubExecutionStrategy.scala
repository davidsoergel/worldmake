package worldmake.executionstrategy

import worldmake._
import scalax.file.Path
import scala.collection.GenMap
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.WorldMakeConfig._
import scalax.io.Resource
import java.io.{IOException, File}
import worldmake.storage.StoredProvenances
import scala.Some
import scala.sys.process.{ProcessLogger, Process}
import worldmake.derivationstrategy.{CallbackNotifier, PollingAction, Notifier}
import edu.umass.cs.iesl.scalacommons.XMLIgnoreDTD
import ExecutionContext.Implicits.global

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

trait QsubRunningInfo extends RunningInfo {
  def jobId: Int

  def workingDir: Path

  def outputPath: Path
}

case class MemoryQsubRunningInfo(jobId: Int, workingDir: Path, outputPath: Path, node: Option[String]) extends QsubRunningInfo

class QsubExecutionStrategy(notifier: Notifier) extends SystemExecutionStrategy with Logging {

  def apply(pr: BlockedProvenance[Path], reifiedScriptF: Future[Successful[String]], reifiedDependenciesF: Future[Iterable[(String, Successful[Any])]]): Future[Successful[Path]] = {
    for (reifiedScript <- reifiedScriptF;
         reifiedDependencies <- reifiedDependenciesF;
         x <- systemExecuteWithArgs(pr.pending(Set(reifiedScript), reifiedDependencies.toMap), reifiedScript, reifiedDependencies.toMap)
    ) yield x
  }

  private def systemExecuteWithArgs(pp: PendingProvenance[Path], reifiedScript: Successful[String], reifiedDependencies: GenMap[String, Successful[_]]): Future[Successful[Path]] = {


    // this path does not yet exist.
    // the derivation may write a single file to it, or create a directory there.
    val outputPath: Path = fileStore.newPath
    //val log: File = (outputPath / "worldmake.log").fileOption.getOrElse(throw new Error("can't create log: " + outputPath / "worldmake.log"))
    //val logWriter = Resource.fromFile(log)


    val dependenciesEnvironment: GenMap[String, String] = reifiedDependencies.mapValues(x => SystemDerivation.toEnvironmentString(x.output))
    val environment: GenMap[String, String] = WorldMakeConfig.globalEnvironment ++ dependenciesEnvironment ++ Map("out" -> outputPath.toAbsolute.path) //, "PATH" -> WorldMakeConfig.globalPath)


    val workingDir = Path.createTempDirectory(dir = WorldMakeConfig.qsubGlobalTempDir)
    val runner = Resource.fromFile(new File((workingDir / "worldmake.runner").toAbsolute.path))
    runner.write(reifiedScript.output.value)

    val envlog = Resource.fromFile(new File((workingDir / "worldmake.environment").toAbsolute.path))
    envlog.write(environment.map({
      case (k, v) => "export " + k + "=" + v
    }).mkString("\n"))

    val qsubScript = Resource.fromFile(new File((workingDir / "worldmake.qsub").toAbsolute.path))
    val stderrLog = (workingDir / "stderr.log").toAbsolute.path
    val stdoutLog = (workingDir / "stdout.log").toAbsolute.path
    val work = workingDir.toAbsolute.path

    // todo make this configurable for different qsub implementations?
    
    qsubScript.write( s"""#!/bin/sh
     |#PBS -e $stderrLog
     |#PBS -o $stdoutLog
     |cd $work
     |/bin/sh -c "source ./worldmake.environment; source ./worldmake.runner"
     |echo $$? > exitcode.log""".stripMargin)

    val qsubLogWriter = new LocalWriteableStringOrFile(WorldMakeConfig.logStore)

    val qsubPb = Process(Seq(WorldMakeConfig.qsub, "-C", "#PBS", "./worldmake.qsub"), workingDir.jfile)

    // any successful output should be written to a file in the output directory, so anything on stdout or stderr is 
    // logging output and should be combined for easier debugging
    val qsubPbLogger = ProcessLogger(
      (o: String) => qsubLogWriter.write(o),
      (e: String) => qsubLogWriter.write(e))

    val qsubPbExitCode = qsubPb ! qsubPbLogger

    val qsubOutput = qsubLogWriter.getString

    if (qsubPbExitCode != 0) {
      logger.warn("Deleting output: " + outputPath)
      outputPath.deleteRecursively()
      logger.warn("Retaining working directory: " + workingDir)
      val prsx = pp.running(new MemoryQsubRunningInfo(0, workingDir, outputPath, None))
      val f = prsx.failed(qsubPbExitCode, Some(qsubLogWriter), Map.empty)

      throw FailedDerivationException(qsubOutput, f)
    }
    val jobIdRE = """^Your job (\d+) \(".+?"\) has been submitted$""".r
    val jobIdRE(jobId) = qsubOutput


    val prs = pp.running(new MemoryQsubRunningInfo(jobId.toInt, workingDir, outputPath, None))

    notifier.request(prs.derivationId)
  }

  /*
  def notifyDone(qi: QsubInfo) {

    // todo: detect retained dependencies like Nix

    val result = ExternalPathArtifact(qi.outputPath)

    val endTime = DateTime.now()

    val exitCode: Int = Resource.fromFile(new File((qi.workingDir / "exitcode.log").toAbsolute.path)).lines().head.toInt

    // copy the log from the qsub file into the database or file store, as needed
    val logWriter = new LocalWriteableStringOrFile(WorldMakeConfig.logStore)
    val logLines = Resource.fromFile(new File((qi.workingDir / "stderr.log").toAbsolute.path)).lines()
    logLines.map(logWriter.write)

    if (exitCode != 0) {
      logger.warn("Deleting output: " + qi.outputPath)
      qi.outputPath.deleteRecursively()
      logger.warn("Retaining working directory: " + qi.workingDir)

      Provenance(Identifier[Provenance[Path]](UUID.randomUUID().toString),
        derivationId = qi.derivedFrom,
        status = ProvenanceStatus.Failure,
        derivedFromNamed = qi.reifiedDependencies,
        derivedFromUnnamed = Set(qi.reifiedScript),
        startTime = qi.startTime,
        endTime = endTime,
        exitCode = Some(exitCode),
        log = Some(logWriter),
        output = None)

      //logger.error(logWriter.get.fold(x => x, y => y.toString))

      throw new FailedDerivationException(logWriter.getString)
    }

    if (WorldMakeConfig.debugWorkingDirectories) {
      logger.warn("Retaining working directory: " + qi.workingDir)
    } else {
      qi.workingDir.deleteRecursively()
    }

    SuccessfulProvenance(Identifier[Provenance[Path]](UUID.randomUUID().toString),
      derivationId = qi.derivedFrom,
      status = ProvenanceStatus.Success,
      derivedFromNamed = qi.reifiedDependencies,
      derivedFromUnnamed = Set(qi.reifiedScript),
      startTime = qi.startTime,
      endTime = endTime,
      statusCode = Some(exitCode),
      log = Some(logWriter),
      output = Some(result))
  }
  */
}


object DetectQsubPollingAction extends PollingAction with Logging {
  def apply(sps: Iterable[StoredProvenances[_]], notifier: CallbackNotifier) {
    try {
      val qstat = new Qstat()
      for (sp <- sps) {
        val jobInfos = sp.running map ((x: RunningProvenance[_]) => (x, x.runningInfo))

        // it is legit for there to be multiple provenances running simultaneously for the same Derivation.  E.g., multiple runs of a stochastic derivation.

        for (jobInfo <- jobInfos) {

          jobInfo match {
            case ((p: RunningProvenance[Any], ri: QsubRunningInfo)) => {

              val exitCode: Option[Int] = try {
                Some(Resource.fromFile(new File((ri.workingDir / "exitcode.log").toAbsolute.path)).lines().head.toInt)
              } catch {
                case e: IOException => None
              }

              val log = try {
                // copy the log from the qsub file into the database or file store, as needed
                val logWriter = new LocalWriteableStringOrFile(WorldMakeConfig.logStore)
                val logLines = Resource.fromFile(new File((ri.workingDir / "stderr.log").toAbsolute.path)).lines()
                logLines.map(logWriter.write)
                Some(logWriter)
              }
              catch {
                case e: IOException => None
              }

              def notifyByExitCode() {
                if (exitCode == Some(0)) {
                  notifier.announceDone(p.completed(0, log, Map.empty, ExternalPathArtifact(ri.outputPath)))
                }
                else {

                  notifier.announceFailed(p.failed(exitCode.getOrElse(-1), log, Map.empty))
                }
              }

              qstat.jobState(ri.jobId) match {
                case Some(QstatJobStatus.QWaiting) => {
                  // do nothing
                }
                case Some(QstatJobStatus.QRunning) => {
                  // do nothing
                  // todo update runninginfo node
                }
                case Some(QstatJobStatus.QCancelled) => {
                  notifier.announceCancelled(p.cancelled(exitCode.getOrElse(-1), log, Map.empty))
                }
                case Some(QstatJobStatus.QDone) => notifyByExitCode()
                case None => notifyByExitCode()
              }
            }
          }
        }
      }
    }
    catch {
      case e: QstatException => logger.error("Qstat Error, ignoring: " + e.getMessage)
    }

  }

}

class QstatException(s: String) extends Exception(s)

class Qstat {
  private val qstatXml = {
    try {
      Process(Seq(WorldMakeConfig.qstat, "-xml")).!!
    }
    catch {
      case e: IOException => throw new QstatException(e.getMessage)
    }
  }
  private val qstatRoot = XMLIgnoreDTD.loadString(qstatXml)
  private val states: Map[Int, QstatJobStatus.QstatJobStatus] = {
    val jobs = qstatRoot \ "queue_info" \ "job_list" ++ qstatRoot \ "job_info" \ "job_list"
    val pairs = for (job <- jobs) yield {
      val state = job.attribute("state").map(_.text)
      val jobId = (job \ "JB_job_number").head.text.toInt
      val mappedState: QstatJobStatus.QstatJobStatus = state match {
        case Some("pending") => QstatJobStatus.QWaiting
        case Some("running") => QstatJobStatus.QRunning
        case Some("cancelled") => QstatJobStatus.QCancelled
        case Some("done") => QstatJobStatus.QDone
        case None => throw new QstatException("Job has no state: " + jobId)
      }
      (jobId, mappedState)
    }
    pairs.toMap
  }


  def jobState(jobId: Int): Option[QstatJobStatus.QstatJobStatus] = states.get(jobId)
}


object QstatJobStatus extends Enumeration {
  type QstatJobStatus = Value

  val QWaiting, QRunning, QCancelled, QDone = Value
}
