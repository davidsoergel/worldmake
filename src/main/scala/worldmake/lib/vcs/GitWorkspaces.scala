/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.lib.vcs

import scalax.file.Path

import scala.sys.process._
import com.typesafe.scalalogging.slf4j.Logging
import worldmake._
import ConstantRecipe._
import java.io.IOException

/**
  * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
  */
object GitWorkspaces extends VcsWorkspaces with Logging {

  def defaultBranchName = "master"
  
   def toUrl(id: String): String = WorldMakeConfig.gitRemoteRoot + (id + ".git")  // add .git because the remote should be bare
 
   def toLocalRepo = new IdentifiableFunction1[String,Path]("Git fetch", toLocalRepoRaw)
  
  private def toLocalRepoRaw(id: String): Path = {
     val p: Path = WorldMakeConfig.gitLocalRoot / (id + ".git")  // add .git because we clone bare
     if (p.isDirectory) {
       logger.debug("Pulling changes to " + id)
       executeWithLog(Seq("git", "fetch"), p)
     } else {
       logger.debug("Cloning " + id)
       executeWithLog(Seq("git", "clone", "--bare", toUrl(id)), WorldMakeConfig.gitLocalRoot)
     }
     p
   }
 
   def get(id: String, requestVersion: String = "latest"): Recipe[ManagedPath] = {
     val version = requestVersion match {
       case "latest" => getLatestVersions(id)("master")
       case v => v
     }
     val args = Map[String, Recipe[_]](
       "localrepo" -> new Recipe1(toLocalRepo, id),
       "version" -> version)
 
     // note git archive can access remote repos, so we could skip the "local" repo, but we do it anyway because
     // a) good idea to store the complete history locally anyway, and
     // b) symmetry with the hg solution
     val scriptRecipe = """mkdir -p $out && cd $localrepo && git archive $version | tar -x -C $out"""
     
     new SystemRecipe(scriptRecipe, args)
   }
 
   private val gitBranchesToChangesetNumber = """^[ \*] (\S+) (\S+) (.*)$""".r
 
   def getLatestVersions(id: String): Map[String, String] = {
     val localrepo = toLocalRepoRaw(id)
 
     logger.debug("Finding latest versions in " + localrepo.toAbsolute.path)
     val pb = Process(Seq("git", "branch", "-v"), localrepo.fileOption) //, environment.toArray: _*)
 
     try {
     pb.lines.map(line => {
       val gitBranchesToChangesetNumber(branch, changeset, logmessage) = line
       logger.debug(line + "  ==>>  " + branch + " -> " + changeset )
       branch -> changeset
     }).toMap
     }
     catch {
       case e: IOException => Map.empty
     }
   }

}
