
package worldmake

import com.typesafe.config.{ConfigFactory, Config}
import scalax.file.Path

import com.typesafe.scalalogging.slf4j.Logging
import com.mongodb.casbah.MongoConnection
import worldmake.storage.casbah.CasbahStorage
import edu.umass.cs.iesl.scalacommons.util.Hash
import java.io.{File, InputStream}
import worldmake.storage.{FileStore, StorageSetter}
import scala.collection.mutable
import edu.umass.cs.iesl.scalacommons.StringUtils
import StringUtils._

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object WorldMake extends Logging {


  def main(worldF: WorldFactory, args: Array[String]) {

    val dbname = args(0)
    StorageSetter(new CasbahStorage(MongoConnection("localhost"), dbname))
    val world = worldF.get

    // temp hack
    // val targets:Map[String,Derivation[_]] = Map("chpat"->EmergeWorld.allChinesePatentsTokenized)

    val command = args(1)
    command match {
      case "make" => {
        val target = args(2)
        //val derivationId = symbolTable.getProperty(target) 
        //val derivationArtifact = Storage.artifactStore.get(derivationId)
        val derivation: Derivation[_] = world(target)
        try {
          val result = derivation.resolveOne
          logger.info("Done: " + result.provenanceId)
          logger.info(result.artifact.value.toString)
        }
        catch {
          case e: FailedDerivationException => {
            logger.error("FAILED.");
            System.exit(1)
          }
        }
      }
      case "status" => {
        val target = args(2)
        //val derivationId = symbolTable.getProperty(target) 
        //val derivationArtifact = Storage.artifactStore.get(derivationId)
        val derivation: Derivation[_] = world(target)
        logger.info(derivation.printTree(""))
      }
      case "showqueue" => {
        val target = args(2)
        //val derivationId = symbolTable.getProperty(target) 
        //val derivationArtifact = Storage.artifactStore.get(derivationId)
        val derivation: Derivation[_] = world(target)
        logger.info("\n"+derivation.getQueue.filterNot(_.isInstanceOf[ConstantDerivation[_]]).map(_.statusLine).mkString("\n"))
      }
      //case "import"
      //case "set"


      // some required derivations have errors.  Are you sure?
      // 1) examine errors
      // 2) compute everything else
      // 3) try again including the errored derivations
    }

  }
}

object WorldMakeConfig {


  val conf: Config = ConfigFactory.load()

  val mercurialLocalRoot: Path = Path.fromString(conf.getString("hglocal"))
  val mercurialRemoteRoot: String = conf.getString("hgremote")
  mercurialLocalRoot.createDirectory(createParents = true, failIfExists = false)

  val globalPath: String = conf.getString("globalpath")

  import scala.collection.JavaConversions._

  val ignoreFilenames: Seq[String] = conf.getStringList("ignoreFilenames")

  def globalEnvironment: Map[String, String] = Map("PATH" -> WorldMakeConfig.globalPath)

  def debugWorkingDirectories: Boolean = conf.getBoolean("debugWorkingDirectories")


  val fileStore = new FileStore(Path.fromString(conf.getString("filestore")))

  val logStore = new FileStore(Path.fromString(conf.getString("logstore")))
  //val artifactStore = new ArtifactStore(conf.getString("artifactstore"))
  //val symbolTable = new Properties("worldmake.symbols")

  val prefixIncrement = "  |"

  val HashType = "SHA-256"

  def WMHash(s: String) = Hash(HashType, s)

  def WMHash(s: InputStream) = Hash(HashType, s)

  def WMHash(s: File) = Hash(HashType, s)

  def WMHashHex(s: String) = Hash.toHex(WMHash(s))

  def WMHashHex(s: InputStream) = Hash.toHex(WMHash(s))

  def WMHashHex(s: File) = Hash.toHex(WMHash(s))

}
