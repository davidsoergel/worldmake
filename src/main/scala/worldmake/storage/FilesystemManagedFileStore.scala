package worldmake.storage

import java.util.UUID
import scalax.file.Path
import worldmake.{ManagedPath, ManagedFileStore}

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class FilesystemManagedFileStore(val rootPath: Path) extends ManagedFileStore {

  rootPath.createDirectory(createParents = true, failIfExists = false)

  val dirStructure = """(...)(......)(.*)""".r

  def newId: Identifier[ManagedPath] = {
    val id = UUID.randomUUID.toString
    new Identifier[ManagedPath](id)
  }

  def all: Iterator[(Identifier[ManagedPath], Path)] = {
    for (a <- rootPath.children().toIterator;
         b <- a.children().toIterator;
         c <- b.children().toIterator) yield {
      val fileId = new Identifier[ManagedPath](a.name + b.name + c.name)
      (fileId, c.toAbsolute)
    }
  }

  def getOrCreate(id: Identifier[ManagedPath]): Path = {
    val dirStructure(a, b, c) = id
    val p: Path = rootPath / a / b
    p.createDirectory(createParents = true, failIfExists = false)
    p / c
  }

  def get(id: Identifier[ManagedPath]): Option[Path] = {
    val dirStructure(a, b, c) = id
    val p: Path = rootPath / a / b / c
    if (p.exists) Some(p) else None
  }

  def exists(id: Identifier[ManagedPath]): Boolean = {
    val dirStructure(a, b, c) = id
    val p: Path = rootPath / a / b / c
    p.exists
  }


  /*
  def newDirectory: Path = {
    val p: Path = newPath
    p.createDirectory(createParents = true, failIfExists = true)
    p
  }*/

  /*
  def get(id:UUID) {
    val dirStructure(a, b, c) = id
    val p: Path = rootPath / a / b / c
    if(p.nonExistent) throw new Error("directory does not exist")
    p
  }*/
}
