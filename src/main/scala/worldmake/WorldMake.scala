
package worldmake

import com.typesafe.config.{ConfigFactory, Config}
import scalax.file.Path

import com.typesafe.scalalogging.slf4j.Logging
import com.mongodb.casbah.MongoConnection
import worldmake.storage.casbah.CasbahStorage
import edu.umass.cs.iesl.scalacommons.util.Hash
import java.io.{File, InputStream}
import worldmake.storage.{Storage, FilesystemManagedFileStore, StorageSetter}

import worldmake.cookingstrategy._
import worldmake.executionstrategy.{LocalExecutionStrategy, DetectQsubPollingAction, QsubExecutionStrategy}
import scala.collection.mutable
import commands._
import scalax.io.{Resource, Output}
import scala.Some

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object WorldMake extends Logging {

  // todo refactor this spaghetti.  The shutdown hooks could be well handled with Cake per Spiewak

  val notifiersForShutdown: mutable.Set[Notifier] = new mutable.HashSet[Notifier]()

  def getStrategy(withNotifiers: Boolean): LifecycleAwareCookingStrategy = {

    val strategy: LifecycleAwareCookingStrategy = WorldMakeConfig.executor match {
      case "local" => {
        val notifierOpt = if (withNotifiers) {
          val notifier = new PollingNotifier(Seq(DetectSuccessPollingAction, DetectFailedPollingAction))
          notifiersForShutdown += notifier
          Some(notifier)
        } else None
        new LifecycleAwareCookingStrategy {
          lazy val fallback = new ComputeNowCookingStrategy(this, LocalExecutionStrategy)
          val tracker = new LifecycleTracker(notifierOpt)
        }
      }
      case "qsub" => {
        val notifierOpt = if (withNotifiers) {
          val notifier = new PollingNotifier(Seq(DetectSuccessPollingAction, DetectFailedPollingAction, DetectQsubPollingAction))
          notifiersForShutdown += notifier
          Some(notifier)
        } else None

        new LifecycleAwareCookingStrategy {
          lazy val fallback = notifierOpt.map(notifier => new ComputeNowCookingStrategy(this, new QsubExecutionStrategy(notifier))).getOrElse(NotAvailableCookingStrategy)
          val tracker = new LifecycleTracker(notifierOpt)
        }
      }
    }

    strategy
  }

  def recipeCommand(r: Recipe[_], command: String, world: World, out: Output) = {
    val strategy = getStrategy(withNotifiers = true)

    val c = new RecipeCommands(world, strategy, out, r)
    import c._
    command match {
      case "show" => showRecipe()
      case "value" => makeRecipe()
      case "deps" => showRecipeDeps()
      case "queue" => showRecipeQueue()
      case "tree" => showRecipeTree()
    }
  }


  def provenanceCommand(target: String, command: String, world: World, out: Output) = {
    val provenances = ProvenanceFinder(world, target)

    //val strategy = getStrategy(withNotifiers = true)

    if (provenances.isEmpty) {
      System.err.println(s"No provenances found for id: $target")
    }
    else {

      val multi = new MultiProvenanceCommands[Any](out, provenances)
      import multi._

      val single = new SingleProvenanceCommands[Any](out, provenances.head)
      import single._


      def singleOnly(c: String, function: () => Unit) {
        if (provenances.size > 1) {
          out.write(s"Command '$c' can only operate on a single Provenance.  Choose one: ")
          showProvenances()
        }
        else function()
      }

      command match {
        case "show" => showProvenances()
        case "showfull" => showProvenanceQueueDetailed()
        case "queue" => singleOnly(command, showProvenanceQueue)
        case "deps" => showProvenanceDeps()
        //case "tree" => singleOnly(command, showProvenanceTree) //ShowTree(world, args(1), getStrategy(withNotifiers = true))
        case "provenance" => singleOnly(command, showProvenanceQueueDetailed)
        case "log" => singleOnly(command, showProvenanceLog)
        case "logfull" => singleOnly(command, showProvenanceFullLog)
        case "blame" => singleOnly(command, showProvenanceBlame) // like showqueue, filtered for failures
        case c => {
          logger.error("Unknown command: " + c)
          ""

        }
      }
    }
  }

  /**
   *
   * @param command
   * @return true if a global command was found
   */
  def globalCommand(command: String, out: Output): Boolean = {
    command match {

      case "clean" => {
        // remove anything that is blocked, pending, failed, cancelled, or running
        Storage.provenanceStore.removeDead
        Storage.provenanceStore.removeZombie
        out.write("Cleaned metadata cache of unsuccessful jobs.")
        true
      }
      case "gc" => {
        val gcdb = Cleanup.gcdb()
        val gcfiles = Cleanup.gcfiles()
        val gclogs = Cleanup.gclogs()

        out.write(s"Garbage-collected metadata, file, and log caches.\n$gcdb\n$gcfiles\n$gclogs\n")
        true
      }

      case "gcdb" => {
        val gcdb = Cleanup.gcdb()
        out.write(s"Garbage-collected metadata cache.\n$gcdb\n")
        true
      }

      case "gcfiles" => {
        val gcfiles = Cleanup.gcfiles()
        out.write(s"Garbage-collected file cache.\n$gcfiles\n")
        true
      }
      case "gclogs" => {
        val gclogs = Cleanup.gclogs()
        out.write(s"Garbage-collected log cache.\n$gclogs\n")
        true
      }
      /*case "makeall" => {
       for(i <- roots) {
         make(i);
       }
     }*/
      //case "import"
      //case "set"


      // some required derivations have errors.  Are you sure?
      // 1) examine errors
      // 2) compute everything else
      // 3) try again including the errored derivations
      case _ => false

    }
  }

  def main(worldF: WorldFactory, args: Array[String]) {

    StorageSetter(new CasbahStorage(MongoConnection(WorldMakeConfig.mongoHost), WorldMakeConfig.mongoDbName, new FilesystemManagedFileStore(Path.fromString(WorldMakeConfig.fileStoreName)), new FilesystemManagedFileStore(Path.fromString(WorldMakeConfig.logStoreName))))
    val world = worldF.get

    val exitcode: Int = try {

      val target = args(0)

      val namedRecipe = world.get(target)

      val command = if (args.length == 1) "show" else args(1)

      // if the first arg is a global command, ignore any superfluous arguments.
      // else if the target names a recipe, apply the command to it
      // else interpret the target as a provenance and apply the command to it

      val out = Resource.fromOutputStream(System.out)

      if (!globalCommand(target, out)) {
        namedRecipe.map(recipeCommand(_, command, world, out)).getOrElse(provenanceCommand(target, command, world, out))
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

  def fileStoreName = conf.getString("filestore")

  //private val fileStore = new FileStore(Path.fromString(conf.getString("filestore")))

  def logStoreName = conf.getString("logstore")

  //private val logStore = new FileStore(Path.fromString(conf.getString("logstore")))
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

// just a marker, really
trait WorldmakeEntity
