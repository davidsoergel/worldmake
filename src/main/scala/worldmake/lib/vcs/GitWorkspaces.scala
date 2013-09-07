package worldmake.lib.vcs

import scalax.file.Path

import scala.sys.process._
import com.typesafe.scalalogging.slf4j.Logging
import worldmake._
import ConstantDerivation._

/**
  * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
  */
object GitWorkspaces extends VcsWorkspaces with Logging {

  def defaultBranchName = "master"
  
   def toUrl(id: String): String = WorldMakeConfig.gitRemoteRoot + id
 
   def toLocalRepo(id: String): Path = {
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
 
   def get(id: String, requestVersion: String = "latest"): Derivation[Path] = {
     val version = requestVersion match {
       case "latest" => getLatestVersions(id)("master")
       case v => v
     }
     val args = Map[String, Derivation[_]](
       "localrepo" -> toLocalRepo(id),
       "version" -> version)
 
     // note git archive can access remote repos, so we could skip the "local" repo, but we do it anyway because
     // a) good idea to store the complete history locally anyway, and
     // b) symmetry with the hg solution
     val scriptDerivation = """mkdir -p $out && cd $localrepo && git archive $version | tar -x -C $out"""
     
     new SystemDerivation(scriptDerivation, args)
   }
 
   private val gitBranchesToChangesetNumber = """^[ \*] (\S+) (\S+) (.*)$""".r
 
   def getLatestVersions(id: String): Map[String, String] = {
     val localrepo = toLocalRepo(id)
 
     logger.debug("Finding latest versions in " + localrepo.toAbsolute.path)
     val pb = Process(Seq("git", "branch", "-v"), localrepo.toAbsolute.fileOption) //, environment.toArray: _*)
 
     pb.lines.map(line => {
       val gitBranchesToChangesetNumber(branch, changeset, logmessage) = line
       logger.debug(line + "  ==>>  " + branch + " -> " + changeset )
       branch -> changeset
     }).toMap
   }

}
