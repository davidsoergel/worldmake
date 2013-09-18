
package worldmake

import com.typesafe.config.{ConfigFactory, Config}
import scalax.file.Path

import com.typesafe.scalalogging.slf4j.Logging
import com.mongodb.casbah.MongoConnection
import worldmake.storage.casbah.CasbahStorage
import edu.umass.cs.iesl.scalacommons.util.Hash
import java.io.{File, InputStream}
import worldmake.storage.{FileStore, StorageSetter}

import scala.concurrent.{Await, ExecutionContext}
import ExecutionContext.Implicits.global
import worldmake.cookingstrategy._
import worldmake.executionstrategy.{LocalExecutionStrategy, DetectQsubPollingAction, QsubExecutionStrategy}
import scala.util.Failure
import scala.util.Success
import scala.concurrent.duration.Duration
import scala.collection.mutable

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object WorldMake extends Logging {

  // todo refactor this spaghetti.  The shutdown hooks could be well handled with Cake per Spiewak

  val notifiersForShutdown: mutable.Set[Notifier] = new mutable.HashSet[Notifier]()

  def getStrategy(withNotifiers: Boolean): LifecycleAwareCookingStrategy = {

    val strategy: LifecycleAwareCookingStrategy = WorldMakeConfig.executor match {
      case "local" => {
        val notifier = new PollingNotifier(Seq(DetectSuccessPollingAction, DetectFailedPollingAction))
        notifiersForShutdown += notifier
        new LifecycleAwareCookingStrategy {
          lazy val fallback = new ComputeNowCookingStrategy(this, LocalExecutionStrategy)
          val tracker = new LifecycleTracker(if(withNotifiers) Some(notifier) else None)
        }
      }
      case "qsub" => {
        val notifier = new PollingNotifier(Seq(DetectSuccessPollingAction, DetectFailedPollingAction, DetectQsubPollingAction))
        notifiersForShutdown += notifier
        new LifecycleAwareCookingStrategy {
          lazy val fallback = new ComputeNowCookingStrategy(this, new QsubExecutionStrategy(notifier))
          val tracker = new LifecycleTracker(if(withNotifiers) Some(notifier) else None)
        }
      }
    }

    strategy
  }

  def main(worldF: WorldFactory, args: Array[String]) {

    //val dbname = args(0)
    StorageSetter(new CasbahStorage(MongoConnection(WorldMakeConfig.mongoHost), WorldMakeConfig.mongoDbName)) //MongoConnection("localhost"), dbname))
    val world = worldF.get


    // temp hack
    // val targets:Map[String,Derivation[_]] = Map("chpat"->EmergeWorld.allChinesePatentsTokenized)


    val exitcode: Int = try {

      val command = args(0)
      command match {
        case "make" => {
          val target = args(1)

          val strategy = getStrategy(withNotifiers = true)

          //val derivationId = symbolTable.getProperty(target) 
          //val derivationArtifact = Storage.artifactStore.get(derivationId)
          val recipe: Recipe[_] = world(target)
          val result = strategy.cookOne(recipe)
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

          Await.result(result, Duration.Inf)

        }
        case "status" => {
          val target = args(1)
          val strategy = getStrategy(withNotifiers = false)
          //val derivationId = symbolTable.getProperty(target) 
          //val derivationArtifact = Storage.artifactStore.get(derivationId)
          val recipe: Recipe[_] = world(target)
          logger.info(strategy.tracker.printTree(recipe, ""))
        }
        case "showqueue" => {
          val target = args(1)
          val strategy = getStrategy(withNotifiers = false)
          //val derivationId = symbolTable.getProperty(target) 
          //val derivationArtifact = Storage.artifactStore.get(derivationId)
          val recipe: Recipe[_] = world(target)
          logger.info("\n" + recipe.queue.filterNot(_.isInstanceOf[ConstantRecipe[Any]]).map(x => strategy.tracker.statusLine(x)).mkString("\n"))

          //logger.info("\n"+derivation.queue.map(x=>strategy.statusLine(x)).mkString("\n")) 

        }
        //case "import"
        //case "set"


        // some required derivations have errors.  Are you sure?
        // 1) examine errors
        // 2) compute everything else
        // 3) try again including the errored derivations


      }
      0
    }
    catch {
      case e: FailedRecipeException => {
        logger.error("FAILED: ", e)
        1
      }
    }
    finally {
      notifiersForShutdown.map(_.shutdown())
    }

    System.exit(exitcode)
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
  def localTempDir: String = conf.getString("localTempDir")

  val fileStore = new FileStore(Path.fromString(conf.getString("filestore")))

  val logStore = new FileStore(Path.fromString(conf.getString("logstore")))
  //val artifactStore = new ArtifactStore(conf.getString("artifactstore"))
  //val symbolTable = new Properties("worldmake.symbols")

  val mongoHost = conf.getString("mongoHost")
  val mongoDbName = conf.getString("mongoDbName")


  val executor = conf.getString("executor")

  val prefixIncrement = "  |"

  val HashType = "SHA-256"

  def WMHash(s: String) = Hash(HashType, s)

  def WMHash(s: InputStream) = Hash(HashType, s)

  def WMHash(s: File) = Hash(HashType, s)

  def WMHashHex(s: String) = Hash.toHex(WMHash(s))

  def WMHashHex(s: InputStream) = Hash.toHex(WMHash(s))

  def WMHashHex(s: File) = Hash.toHex(WMHash(s))

}
