package worldmake.lib.vcs

import scalax.file.Path

import scala.sys.process._
import com.typesafe.scalalogging.slf4j.Logging
import worldmake._

import ConstantDerivation._
/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object MercurialWorkspaces extends VcsWorkspaces with Logging {

  def defaultBranchName = "default"
  
  def toUrl(id: String): String = WorldMakeConfig.mercurialRemoteRoot + id

  def toLocalRepo(id: String, pathType:String="worldmake.UnknownTypedPath"): TypedPath = {
    val p: Path = WorldMakeConfig.mercurialLocalRoot / id
    if (p.isDirectory) {
      logger.debug("Pulling change to " + id)
      executeWithLog(Seq("hg", "pull"), p)
    } else {
      logger.debug("Cloning " + id)
      executeWithLog(Seq("hg", "clone", toUrl(id)), WorldMakeConfig.mercurialLocalRoot)
    }
    TypedPathMapper.map(pathType,p)
  }

  def get(id: String, requestVersion: String = "latest"): Derivation[TypedPath] = {
    val version = requestVersion match {
      case "latest" => getLatestVersions(id)("default")
      case v => v
    }
    val args = Map[String, Derivation[_]](
      "localrepo" ->toLocalRepo(id),
      "version" -> version)

    val scriptDerivation : Derivation[String] ="""mkdir -p $out && cd $out && hg archive -R $localrepo -r $version ."""
    
    new SystemDerivation(scriptDerivation, args)
  }

  private val hgBranchesToChangesetNumber = """(\S+)\s*(\d+):(\S*)( \(.*\))?""".r

  def getLatestVersions(id: String): Map[String, String] = {
    val localrepo = toLocalRepo(id)

    logger.debug("Finding latest versions in " + localrepo.abspath)
    val pb = Process(Seq("hg", "branches"), localrepo.fileOption) //, environment.toArray: _*)

    pb.lines.map(line => {
      val hgBranchesToChangesetNumber(branch, local, changeset, status) = line
      logger.debug(line + "  ==>>  " + branch + " -> " + changeset )
      branch -> changeset
    }).toMap
  }

  

}
