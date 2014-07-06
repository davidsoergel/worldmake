/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.lib.vcs

import worldmake.{ManagedPath, TypedPathReference, Recipe, WorldMakeConfig}
import scalax.file.Path
import scala.sys.process.{Process, ProcessLogger}
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait VcsWorkspaces extends Logging {
  def defaultBranchName : String
  
  def toUrl(id: String): String

  //def toLocalRepo(id: String): Path

  /**
   * A Derivation that extracts the requested version from the VCS repository to the output path.
   * @param id
   * @param requestVersion
   * @return
   */
  def get(id: String, requestVersion: String = "latest"): Recipe[ManagedPath]

  def getLatestVersions(id: String): Map[String, String]

  def executeWithLog(command: Seq[String], workingDir: Path) {

    val pb = Process(command, workingDir.toAbsolute.fileOption) //, environment.toArray: _*)

    logger.debug("in " + workingDir.toAbsolute.path + ", executing " + command.mkString(" "))

    // any successful output should be written to a file in the output directory, so anything on stdout or stderr is 
    // logging output and should be combined for easier debugging
    val pbLogger = ProcessLogger(
      (o: String) => logger.debug(o),
      (e: String) => logger.warn(e))

    val exitCode = pb ! pbLogger

  }
}
