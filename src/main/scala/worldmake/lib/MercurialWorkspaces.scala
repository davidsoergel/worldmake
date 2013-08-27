package worldmake.lib

import scalax.file.Path

import scala.sys.process._
import com.typesafe.scalalogging.slf4j.Logging
import worldmake._

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object MercurialWorkspaces extends Logging {
  def toUrl(id: String): String = WorldMakeConfig.mercurialRemoteRoot + id

  def toLocalRepo(id: String): Path = {
    val p: Path = WorldMakeConfig.mercurialLocalRoot / id
    if (p.isDirectory) {
      logger.debug("Pulling change to " + id)
      executeWithLog(Seq("hg", "pull"), p)
    } else {
      logger.debug("Cloning " + id)
      executeWithLog(Seq("hg", "clone", toUrl(id)), WorldMakeConfig.mercurialLocalRoot)
    }
    p
  }

  def get(id: String, requestVersion: String = "latest"): Derivation[Path] = {
    val version = requestVersion match {
      case "latest" => getLatestVersions(id)("default")
      case v => v
    }
    val args = Map[String, Derivation[_]](
      "localrepo" ->
        ConstantDerivation(
          ConstantProvenance(
            ExternalPathArtifact(
              toLocalRepo(id)))),
      "version" -> ConstantDerivation(ConstantProvenance(StringArtifact(version))))

    val scriptDerivation = ConstantDerivation(ConstantProvenance(StringArtifact( """cd $out && hg archive -R $localrepo -r $version .""")))
    
    new SystemDerivation(scriptDerivation, args)
  }

  val hgBranchesToChangesetNumber = """(\S+)\s*(\d+):(\S*)( \(.*\))?""".r

  def getLatestVersions(id: String): Map[String, String] = {
    val localrepo = toLocalRepo(id)

    logger.debug("Finding latest versions in " + localrepo.toAbsolute.path)
    val pb = Process(Seq("hg", "branches"), localrepo.toAbsolute.fileOption) //, environment.toArray: _*)

    pb.lines.map(line => {
      val hgBranchesToChangesetNumber(branch, local, changeset, status) = line
      logger.debug(line + "  ==>>  " + branch + " -> " + changeset )
      branch -> changeset
    }).toMap
  }

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
