package worldmake.storage

import java.util.UUID
import scalax.file.Path
import worldmake.FilenameGenerator

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class FileStore(val rootPath: Path) extends FilenameGenerator {

  rootPath.createDirectory(createParents = true,failIfExists = false)
  
  val dirStructure = """(...)(.....)(.*)""".r

  def newPath: Path = {
    val id = UUID.randomUUID.toString
    val dirStructure(a, b, c) = id
    val p: Path = rootPath / a / b / c
    p.createDirectory(createParents = true, failIfExists = true)
    p
  }
  
  /*
  def get(id:UUID) {
    val dirStructure(a, b, c) = id
    val p: Path = rootPath / a / b / c
    if(p.nonExistent) throw new Error("directory does not exist")
    p
  }*/
}
