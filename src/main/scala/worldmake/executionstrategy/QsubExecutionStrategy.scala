package worldmake.executionstrategy

import worldmake._
import scalax.file.Path
import scala.collection.GenMap
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.slf4j.Logging
import worldmake.WorldMakeConfig._
import scalax.io.Resource
import java.io.{IOException, File}
import worldmake.storage.{Identifier, ManagedPathArtifact, Storage, StoredProvenancesForRecipe}
import scala.Some
import scala.sys.process.{ProcessLogger, Process}
import worldmake.cookingstrategy.{CallbackNotifier, PollingAction, Notifier}
import edu.umass.cs.iesl.scalacommons.XMLIgnoreDTD
import ExecutionContext.Implicits.global
import scalax.file.defaultfs.DefaultPath

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

trait QsubRunningInfo extends RunningInfo {
  def jobId: Int

  def workingDir: DefaultPath

  def outputPath: ManagedPath

  def infoBlock : String = s"""
  |       qsub ID: ${jobId}
  |   Working Dir: ${workingDir.toAbsolute.path}
  |   Output Path: ${outputPath.abspath}
  | """.stripMargin
  //def requestedType: String
}

case class MemoryQsubRunningInfo(jobId: Int, workingDir: DefaultPath, outputPath: ManagedPath, node: Option[String]) extends QsubRunningInfo  //, requestedType:String

class QsubExecutionStrategy(notifier: Notifier) extends SystemExecutionStrategy with Logging {

  def apply(pr: BlockedProvenance[ManagedPath], reifiedScriptF: Future[Successful[String]], reifiedDependenciesF: Future[Iterable[(String, Successful[Any])]]): Future[Successful[ManagedPath]] = {
    for (reifiedScript <- reifiedScriptF;
         reifiedDependencies <- reifiedDependenciesF;
         x <- systemExecuteWithArgs(pr.pending(Set(reifiedScript), reifiedDependencies.toMap), reifiedScript, reifiedDependencies.toMap)
    ) yield x
  }

  private def systemExecuteWithArgs(pp: PendingProvenance[ManagedPath], reifiedScript: Successful[String], reifiedDependencies: GenMap[String, Successful[_]]): Future[Successful[ManagedPath]] = {


    // this path does not yet exist.
    // the derivation may write a single file to it, or create a directory there.
    val outputId: Identifier[ManagedPath] = Storage.fileStore.newId
    val outputPath: Path = Storage.fileStore.getOrCreate(outputId)
    //val log: File = (outputPath / "worldmake.log").fileOption.getOrElse(throw new Error("can't create log: " + outputPath / "worldmake.log"))
    //val logWriter = Resource.fromFile(log)


    val dependenciesEnvironment: GenMap[String, String] = reifiedDependencies.mapValues(_.output.environmentString)
    val environment: GenMap[String, String] = WorldMakeConfig.globalEnvironment ++ dependenciesEnvironment ++ Map("out" -> outputPath.toAbsolute.path) //, "PATH" -> WorldMakeConfig.globalPath)


    val workingDir = Path.createTempDirectory(dir = WorldMakeConfig.qsubGlobalTempDir, deleteOnExit = !WorldMakeConfig.debugWorkingDirectories)
    val runner = Resource.fromFile((workingDir / "worldmake.runner").toRealPath().jfile)
    //http://stackoverflow.com/questions/821396/aborting-a-shell-script-if-any-command-returns-a-non-zero-value
    runner.write(
      """#!/bin/bash
        |set -e
        |set -o pipefail
        |
        |""".stripMargin)
    runner.write(reifiedScript.output.value)
    runner.write("\n")

    val envlog = Resource.fromFile((workingDir / "worldmake.environment").toRealPath().jfile)
    envlog.write(environment.map({
      case (k, v) => "export " + k + "=" + v
    }).mkString("\n"))
    envlog.write("\n")

    val qsubScript = Resource.fromFile((workingDir / "worldmake.qsub").toRealPath().jfile)
    val stderrLog = (workingDir / "stderr.log").toAbsolute.path
    val stdoutLog = (workingDir / "stdout.log").toAbsolute.path
    val work = workingDir.toAbsolute.path

    // todo make this configurable for different qsub implementations?
    
    qsubScript.write( s"""#!/bin/sh
     |#PBS -e $stderrLog
     |#PBS -o $stdoutLog
     |cd $work
     |/bin/bash -c 'source ./worldmake.environment; /bin/bash ./worldmake.runner; echo $$? > exitcode.log'
     |
     |""".stripMargin)

    val qsubLogWriter = new LocalWriteableStringOrManagedFile(Storage.logStore)

    val qsubPb = Process(Seq(WorldMakeConfig.qsub, "-C", "#PBS", "./worldmake.qsub"), workingDir.jfile)

    // any successful output should be written to a file in the output directory, so anything on stdout or stderr is 
    // logging output and should be combined for easier debugging
    val qsubPbLogger = ProcessLogger(
      (o: String) => qsubLogWriter.write(o),
      (e: String) => qsubLogWriter.write(e))

    val qsubPbExitCode = qsubPb ! qsubPbLogger

    val qsubOutput = qsubLogWriter.getString

   // val requestedType = classManifest[T].toString() //.getName
    
    if (qsubPbExitCode != 0) {
      logger.warn("Deleting output: " + outputPath)
      outputPath.deleteRecursively()
      logger.warn("Retaining working directory: " + workingDir)
      val prsx = pp.running(new MemoryQsubRunningInfo(0, workingDir, ManagedPath(outputId), None)) //,requestedType))
      val f = prsx.failed(qsubPbExitCode, Some(qsubLogWriter), Map.empty)

      throw FailedRecipeException(qsubOutput, f)
    }
    val jobIdRE = """^Your job (\d+) \(".+?"\) has been submitted$""".r
    val jobIdRE(jobId) = qsubOutput


    val prs = pp.running(new MemoryQsubRunningInfo(jobId.toInt, workingDir, ManagedPath(outputId), None)) //,requestedType))

    notifier.request[ManagedPath](prs.recipeId)
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

      Provenance(Identifier[Provenance[TypedPath]](UUID.randomUUID().toString),
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

    SuccessfulProvenance(Identifier[Provenance[TypedPath]](UUID.randomUUID().toString),
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
  def apply(sps: Iterable[StoredProvenancesForRecipe[_]], notifier: CallbackNotifier) {
    try {
      val qstat = new Qstat()
      for (sp <- sps) {
        val jobInfos = sp.running map ((x: RunningProvenance[_]) => (x, x.runningInfo))

        // it is legit for there to be multiple provenances running simultaneously for the same Derivation.  E.g., multiple runs of a stochastic derivation.

        for (jobInfo <- jobInfos) {

          jobInfo match {
            case ((p: RunningProvenance[Any], ri: QsubRunningInfo)) => {

              val exitCode: Option[Int] = try {
                Resource.fromFile((ri.workingDir / "exitcode.log").toRealPath().jfile).lines().headOption.map(_.toInt)
              } catch {
                case e: IOException =>  { logger.error("Error collecting exit code", e); None }
              }

              def collectLog = try {
                // copy the log from the qsub file into the database or file store, as needed
                val logWriter = new LocalWriteableStringOrManagedFile(Storage.logStore)
                val logLines = Resource.fromFile((ri.workingDir / "stderr.log").jfile ).lines()
                logger.debug(s"Found ${logLines.size} log lines.")
                for(s <- logLines) { logWriter.write(s+"\n") }
                logger.debug(s"Wrote ${logWriter.count} characters of logging output.")
                Some(logWriter)
              }
              catch {
                case e: IOException => { logger.error("Error collecting log output", e); None }
              }

              def notifyByExitCode() {
                if (exitCode == Some(0)) {
                  logger.debug("Qstat reported job done with exit code 0: " + p.provenanceId)
                  
                  //notifier.announceDone(p.completed(0, log, Map.empty, TypedPathArtifact(TypedPathMapper.map(ri.requestedType, ri.outputPath))))

                  notifier.announceDone(p.completed(0, collectLog, Map.empty, ManagedPathArtifact( ri.outputPath)))
                }
                else {
                  notifier.announceFailed(p.failed(exitCode.getOrElse(-1), collectLog, Map.empty))
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
                  notifier.announceCancelled(p.cancelled(exitCode.getOrElse(-1), collectLog, Map.empty))
                }
                case Some(QstatJobStatus.QDone) => notifyByExitCode()
                case None => notifyByExitCode()
              }
            }

            case ((p: RunningProvenance[Any], ri: RunningInfo)) => {
              // ignore running jobs that are not qsub jobs
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

class Qstat extends Logging {
  private val qstatXml = {
    try {
      Process(Seq(WorldMakeConfig.qstat, "-xml")).!!
    }
    catch {
      case e: IOException => throw new QstatException(e.getMessage)
    }
  }
  private val qstatRoot = XMLIgnoreDTD.loadString(qstatXml)
  logger.debug(qstatXml)
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
