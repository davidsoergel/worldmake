package worldmake

import scalax.file.Path
import scala.concurrent.{ExecutionContext, Future, Promise}
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.GenMap
import org.joda.time.DateTime
import worldmake.WorldMakeConfig._
import worldmake.storage.Identifier
import scala.Some
import scalax.io.Resource
import java.io.File
import scala.sys.process.{Process, ProcessLogger}
import java.util.UUID
import scala.collection.mutable
import ExecutionContext.Implicits.global

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait SystemExecutionStrategy {
  def apply(derivedFrom: Identifier[Derivation[Path]], scriptF: Future[Successful[String]], env: Future[Iterable[(String, Successful[Any])]]): Future[Successful[Path]]

}

case class QsubInfo(derivedFrom: Identifier[Derivation[Path]], reifiedScript: Successful[String], reifiedDependencies: GenMap[String, Successful[_]], startTime: DateTime, jobId: Integer, workingDir: Path, outputPath: Path, pr: Promise[Successful[Path]])

object QsubExecutionStrategy extends SystemExecutionStrategy with Logging {

  val qsubJobsUnderway: mutable.Map[Identifier[Derivation[Path]], QsubInfo] = mutable.HashMap[Identifier[Derivation[Path]], QsubInfo]() // todo mongodb table

  def apply(derivedFrom: Identifier[Derivation[Path]], reifiedScriptF: Future[Successful[String]], reifiedDependenciesF: Future[Iterable[(String, Successful[Any])]]): Future[Successful[Path]] = {

    for (reifiedScript <- reifiedScriptF;
         reifiedDependencies <- reifiedDependenciesF;
         x <- systemExecuteWithArgs(derivedFrom, reifiedScript, reifiedDependencies.toMap)
    ) yield x
  }

  private def systemExecuteWithArgs(derivedFrom: Identifier[Derivation[Path]], reifiedScript: Successful[String], reifiedDependencies: GenMap[String, Successful[_]]): Future[Successful[Path]] = {

    val startTime = DateTime.now()

    // this path does not yet exist.
    // the derivation may write a single file to it, or create a directory there.
    val outputPath: Path = fileStore.newPath
    //val log: File = (outputPath / "worldmake.log").fileOption.getOrElse(throw new Error("can't create log: " + outputPath / "worldmake.log"))
    //val logWriter = Resource.fromFile(log)


    val dependenciesEnvironment: GenMap[String, String] = reifiedDependencies.mapValues(x => SystemDerivation.toEnvironmentString(x.artifact))
    val environment: GenMap[String, String] = WorldMakeConfig.globalEnvironment ++ dependenciesEnvironment ++ Map("out" -> outputPath.toAbsolute.path) //, "PATH" -> WorldMakeConfig.globalPath)


    val workingDir = Path.createTempDirectory(dir = WorldMakeConfig.qsubGlobalTempDir)
    val runner = Resource.fromFile(new File((workingDir / "worldmake.runner").toAbsolute.path))
    runner.write(reifiedScript.artifact.value)

    val envlog = Resource.fromFile(new File((workingDir / "worldmake.environment").toAbsolute.path))
    envlog.write(environment.map({
      case (k, v) => "export " + k + "=" + v
    }).mkString("\n"))

    val qsubScript = Resource.fromFile(new File((workingDir / "worldmake.qsub").toAbsolute.path))
val stderrLog = (workingDir / "stderr.log").toAbsolute.path
    val stdoutLog = (workingDir / "stdout.log").toAbsolute.path
    val work = workingDir.toAbsolute.path
    
    qsubScript.write(s"""#/bin/sh
     |#PBS -e $stderrLog
     |#PBS -o $stdoutLog
     |cd $work
     |source ./worldmake.environment
     |/bin/sh ./worldmake.runner
     |echo $$? > exitcode.log""".stripMargin)
    
    val qsubLogWriter = new LocalWriteableStringOrFile(WorldMakeConfig.logStore)

    val qsubPb = Process(Seq(WorldMakeConfig.qsub, "./worldmake.qsub"), workingDir.jfile)

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

      throw new FailedDerivationException("qsub failed: " + qsubOutput)
    }
    val jobIdRE = """^Your job (\d+) \(".+?"\) has been submitted$""".r
    val jobIdRE(jobId) = qsubOutput
    val promise = Promise[Successful[Path]]
    val qsubInfo = new QsubInfo(derivedFrom, reifiedScript, reifiedDependencies, startTime, jobId.toInt, workingDir, outputPath, promise)
    qsubJobsUnderway.put(derivedFrom, qsubInfo)
    promise.future
  }

  def notifyDone(qi: QsubInfo) {

    // todo: detect retained dependencies like Nix

    val result = ExternalPathArtifact(qi.outputPath)

    val endTime = DateTime.now()

    val exitCode : Int = Resource.fromFile(new File((qi.workingDir / "exitcode.log").toAbsolute.path)).lines().head.toInt
    
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
        statusCode = Some(exitCode),
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
}


object LocalExecutionStrategy extends SystemExecutionStrategy with Logging {
  def apply(derivedFrom: Identifier[Derivation[Path]], reifiedScriptF: Future[Successful[String]], reifiedDependenciesF: Future[Iterable[(String, Successful[Any])]]): Future[Successful[Path]] = {

    for (reifiedScript <- reifiedScriptF;
         reifiedDependencies <- reifiedDependenciesF
    ) yield systemExecuteWithArgs(derivedFrom, reifiedScript, reifiedDependencies.toMap)
  }

  private def systemExecuteWithArgs(derivedFrom: Identifier[Derivation[Path]], reifiedScript: Successful[String], reifiedDependencies: GenMap[String, Successful[_]]): Successful[Path] = {

    val startTime = DateTime.now()

    // this path does not yet exist.
    // the derivation may write a single file to it, or create a directory there.
    val outputPath: Path = fileStore.newPath

    val workingDir = Path.createTempDirectory()
    //val log: File = (outputPath / "worldmake.log").fileOption.getOrElse(throw new Error("can't create log: " + outputPath / "worldmake.log"))
    //val logWriter = Resource.fromFile(log)

    val logWriter = new LocalWriteableStringOrFile(WorldMakeConfig.logStore)


    val dependenciesEnvironment: GenMap[String, String] = reifiedDependencies.mapValues(x => SystemDerivation.toEnvironmentString(x.artifact))
    val environment: GenMap[String, String] = WorldMakeConfig.globalEnvironment ++ dependenciesEnvironment ++ Map("out" -> outputPath.toAbsolute.path) //, "PATH" -> WorldMakeConfig.globalPath)

    val runner = Resource.fromFile(new File((workingDir / "worldmake.runner").toAbsolute.path))
    runner.write(reifiedScript.artifact.value)

    val envlog = Resource.fromFile(new File((workingDir / "worldmake.environment").toAbsolute.path))
    envlog.write(environment.map({
      case (k, v) => k + " = " + v
    }).mkString("\n"))


    val pb = Process(Seq("/bin/sh", "./worldmake.runner"), workingDir.jfile, environment.toArray: _*)

    // any successful output should be written to a file in the output directory, so anything on stdout or stderr is 
    // logging output and should be combined for easier debugging
    val pbLogger = ProcessLogger(
      (o: String) => logWriter.write(o),
      (e: String) => logWriter.write(e))

    val exitCode = pb ! pbLogger

    // todo: detect retained dependencies like Nix

    val result = ExternalPathArtifact(outputPath)

    val endTime = DateTime.now()

    if (exitCode != 0) {
      logger.warn("Deleting output: " + outputPath)
      outputPath.deleteRecursively()
      logger.warn("Retaining working directory: " + workingDir)

      Provenance(Identifier[Provenance[Path]](UUID.randomUUID().toString),
        derivationId = derivedFrom,
        status = ProvenanceStatus.Failure,
        derivedFromNamed = reifiedDependencies,
        derivedFromUnnamed = Set(reifiedScript),
        startTime = startTime,
        endTime = endTime,
        statusCode = Some(exitCode),
        log = Some(logWriter),
        output = None)

      //logger.error(logWriter.get.fold(x => x, y => y.toString))

      throw new FailedDerivationException(logWriter.getString)
    }

    if (WorldMakeConfig.debugWorkingDirectories) {
      logger.warn("Retaining working directory: " + workingDir)
    } else {
      workingDir.deleteRecursively()
    }

    SuccessfulProvenance(Identifier[Provenance[Path]](UUID.randomUUID().toString),
      derivationId = derivedFrom,
      status = ProvenanceStatus.Success,
      derivedFromNamed = reifiedDependencies,
      derivedFromUnnamed = Set(reifiedScript),
      startTime = startTime,
      endTime = endTime,
      statusCode = Some(exitCode),
      log = Some(logWriter),
      output = Some(result))
  }
}
