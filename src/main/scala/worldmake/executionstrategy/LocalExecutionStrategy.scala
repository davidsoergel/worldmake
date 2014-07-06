/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.executionstrategy

import com.typesafe.scalalogging.slf4j.Logging
import worldmake._
import scalax.file.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.GenMap
import worldmake.WorldMakeConfig._
import scalax.io.Resource
import java.io.File
import scala.Some
import scala.sys.process.{ProcessLogger, Process}
import ExecutionContext.Implicits.global
import scala.reflect.runtime.universe._
import worldmake.storage.{Identifier, ManagedPathArtifact, Storage}
import scalax.file.defaultfs.DefaultPath

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */

object LocalExecutionStrategy extends SystemExecutionStrategy with Logging {
  
  def apply(pr: BlockedProvenance[ManagedPath], reifiedScriptF: Future[Successful[String]], reifiedDependenciesF: Future[Iterable[(String, Successful[Any])]]): Future[Successful[ManagedPath]] = {
    for (reifiedScript <- reifiedScriptF;
         reifiedDependencies <- reifiedDependenciesF
    ) yield {
      val reifiedDependenciesM = reifiedDependencies.toMap
      systemExecuteWithArgs(pr.pending(Set(reifiedScript), reifiedDependenciesM), reifiedScript, reifiedDependenciesM)
    }
  }

  private def systemExecuteWithArgs(pp: PendingProvenance[ManagedPath], reifiedScript: Successful[String], reifiedDependencies: GenMap[String, Successful[_]]): Successful[ManagedPath] = {

    // this path does not yet exist.
    // the derivation may write a single file to it, or create a directory there.
    val outputId: Identifier[ManagedPath] = Storage.fileStore.newId
    val outputPath: Path = Storage.fileStore.getOrCreate(outputId)

    val workingDir: DefaultPath = Path.createTempDirectory(dir = WorldMakeConfig.localTempDir, deleteOnExit = !WorldMakeConfig.debugWorkingDirectories)
    //val log: File = (outputPath / "worldmake.log").fileOption.getOrElse(throw new Error("can't create log: " + outputPath / "worldmake.log"))
    //val logWriter = Resource.fromFile(log)

    val logWriter = new LocalWriteableStringOrManagedFile(Storage.logStore)

    val dependenciesEnvironment: GenMap[String, String] = reifiedDependencies.mapValues(_.output.environmentString)

    val environment: GenMap[String, String] = WorldMakeConfig.globalEnvironment ++ dependenciesEnvironment ++ Map("out" -> outputPath.toAbsolute.path) //, "PATH" -> WorldMakeConfig.globalPath)

    val runner = Resource.fromFile((workingDir / "worldmake.runner").toRealPath().jfile)
    runner.write(reifiedScript.output.value)

    val envlog = Resource.fromFile((workingDir / "worldmake.environment").toRealPath().jfile)
    envlog.write(environment.map({
      case (k, v) => k + " = " + v
    }).mkString("\n"))

    val pb = Process(Seq("/bin/sh", "./worldmake.runner"), workingDir.jfile, environment.toArray: _*)

    val prs = pp.running(new MemoryLocalRunningInfo(workingDir)) // process ID not available


    // any successful output should be written to a file in the output directory, so anything on stdout or stderr is 
    // logging output and should be combined for easier debugging
    val pbLogger = ProcessLogger(
      (o: String) => logWriter.write(o),
      (e: String) => logWriter.write(e))

    val exitCode = pb ! pbLogger

    // todo: detect retained dependencies like Nix

    /*
    val requestedType = {
      classManifest[T].toString //match { case TypeRef(pre, sym, args) => args }
    }
    val result = TypedPathArtifact[T](TypedPathMapper.map(requestedType, outputPath)) //TypedPathArtifact(outputPath)
*/
    val result = ManagedPathArtifact(ManagedPath(outputId))
    
    if (exitCode != 0) {
      logger.warn("Deleting output: " + outputPath)
      outputPath.deleteRecursively()

      logger.warn("Retaining working directory: " + workingDir)

      val f = prs.failed(exitCode, Some(logWriter), Map.empty)

      throw FailedRecipeException(logWriter.getString, f)
    }

    if (WorldMakeConfig.debugWorkingDirectories) {
      logger.warn("Retaining working directory: " + workingDir)
    } else {
      workingDir.deleteRecursively()
    }

    prs.completed(exitCode, Some(logWriter), Map.empty, result)

  }
}
