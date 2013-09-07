
package worldmake

import com.typesafe.config.{ConfigFactory, Config}
import scalax.file.Path

import com.typesafe.scalalogging.slf4j.Logging
import com.mongodb.casbah.MongoConnection
import worldmake.storage.casbah.CasbahStorage
import edu.umass.cs.iesl.scalacommons.util.Hash
import java.io.{File, InputStream}
import worldmake.storage.{FileStore, StorageSetter}

import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import worldmake.derivationstrategy._
import worldmake.executionstrategy.{DetectQsubPollingAction, QsubExecutionStrategy}
import scala.util.Failure
import scala.util.Success

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object WorldMake extends Logging {


  def main(worldF: WorldFactory, args: Array[String]) {

    //val dbname = args(0)
    StorageSetter(new CasbahStorage(MongoConnection(WorldMakeConfig.mongoHost), WorldMakeConfig.mongoDbName)) //MongoConnection("localhost"), dbname))
    val world = worldF.get

    // temp hack
    // val targets:Map[String,Derivation[_]] = Map("chpat"->EmergeWorld.allChinesePatentsTokenized)

    val notifier = new PollingNotifier(Seq(DetectSuccessPollingAction, DetectFailedPollingAction, DetectQsubPollingAction))

    try {

      val strategy: LifecycleAwareFutureDerivationStrategy = new LifecycleAwareFutureDerivationStrategy {
        lazy val fallback = new ComputeFutureDerivationStrategy(this, new QsubExecutionStrategy(notifier))
        val tracker = new LifecycleTracker(notifier)
      }

      val command = args(0)
      command match {
        case "make" => {
          val target = args(1)
          //val derivationId = symbolTable.getProperty(target) 
          //val derivationArtifact = Storage.artifactStore.get(derivationId)
          val derivation: Derivation[_] = world(target)
          try {
            val result = strategy.resolveOne(derivation)

            result onComplete {
              case Success(x) => {
                logger.info("Done: " + x.provenanceId)
                logger.info(x.output.value.toString)
              }
              case Failure(t) => {
                logger.error("Error", t)
                throw t
              }
            }

          }
          catch {
            case e: FailedDerivationException => {
              logger.error("FAILED: " , e)
              System.exit(1)
            }
          }
        }
        case "status" => {
          val target = args(1)
          //val derivationId = symbolTable.getProperty(target) 
          //val derivationArtifact = Storage.artifactStore.get(derivationId)
          val derivation: Derivation[_] = world(target)
          logger.info(strategy.tracker.printTree(derivation, ""))
        }
        case "showqueue" => {
          val target = args(1)
          //val derivationId = symbolTable.getProperty(target) 
          //val derivationArtifact = Storage.artifactStore.get(derivationId)
          val derivation: Derivation[_] = world(target)
          logger.info("\n" + derivation.queue.filterNot(_.isInstanceOf[ConstantDerivation[Any]]).map(x => strategy.tracker.statusLine(x)).mkString("\n"))

          //logger.info("\n"+derivation.queue.map(x=>strategy.statusLine(x)).mkString("\n"))
        }
        //case "import"
        //case "set"


        // some required derivations have errors.  Are you sure?
        // 1) examine errors
        // 2) compute everything else
        // 3) try again including the errored derivations
      }
    } finally {
      notifier.shutdown()
      System.exit(0)
    }
  }
}

object WorldMakeConfig {


  val conf: Config = ConfigFactory.load()

  val mercurialLocalRoot: Path = Path.fromString(conf.getString("hglocal"))
  val mercurialRemoteRoot: String = conf.getString("hgremote")
  mercurialLocalRoot.createDirectory(createParents = true, failIfExists = false)


  val gitLocalRoot: Path = Path.fromString(conf.getString("gitlocal"))
  val gitRemoteRoot: String = conf.getString("gitremote")
  gitLocalRoot.createDirectory(createParents = true, failIfExists = false)


  val globalPath: String = conf.getString("globalpath")

  import scala.collection.JavaConversions._

  val ignoreFilenames: Seq[String] = conf.getStringList("ignoreFilenames")

  def globalEnvironment: Map[String, String] = Map("PATH" -> WorldMakeConfig.globalPath)

  def debugWorkingDirectories: Boolean = conf.getBoolean("debugWorkingDirectories")

  def retryFailures: Boolean = conf.getBoolean("retryFailures")
  
  def qsub: String = conf.getString("qsub")

  def qstat: String = conf.getString("qstat")

  // a scratch directory available from all grid nodes
  def qsubGlobalTempDir: String = conf.getString("qsubGlobalTempDir")

  val fileStore = new FileStore(Path.fromString(conf.getString("filestore")))

  val logStore = new FileStore(Path.fromString(conf.getString("logstore")))
  //val artifactStore = new ArtifactStore(conf.getString("artifactstore"))
  //val symbolTable = new Properties("worldmake.symbols")

  val mongoHost = conf.getString("mongoHost")
  val mongoDbName = conf.getString("mongoDbName")
  
  val prefixIncrement = "  |"

  val HashType = "SHA-256"

  def WMHash(s: String) = Hash(HashType, s)

  def WMHash(s: InputStream) = Hash(HashType, s)

  def WMHash(s: File) = Hash(HashType, s)

  def WMHashHex(s: String) = Hash.toHex(WMHash(s))

  def WMHashHex(s: InputStream) = Hash.toHex(WMHash(s))

  def WMHashHex(s: File) = Hash.toHex(WMHash(s))

}
