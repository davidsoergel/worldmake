package worldmake.storage

import java.util.UUID
import scalax.file.Path
import worldmake.{ManagedPath, ManagedFileStore}
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
class FilesystemManagedFileStore(val rootPath: Path) extends ManagedFileStore with Logging {
  for(s <- rootPath.segments.tail) {
    logger.info("RootPath segment: " + s) 
    require (!s.contains("/"))}

  rootPath.createDirectory(createParents = true, failIfExists = false)

  val dirStructure = """(...)(......)(.*)""".r

  def newId: Identifier[ManagedPath] = {
    val ids = UUID.randomUUID.toString
    new Identifier[ManagedPath](ids)
  }

  def all: Iterator[(Identifier[ManagedPath], Path)] = {
    for (a <- rootPath.children().toIterator if a.isDirectory;
         b <- a.children().toIterator;
         c <- b.children().toIterator) yield {
      val fileId = new Identifier[ManagedPath](a.name + b.name + c.name)
      (fileId, c.toAbsolute)
    }
  }

  def getOrCreate(id: Identifier[ManagedPath]): Path = {
    val dirStructure(a, b, c) = id.s
    val p: Path = rootPath / a / b
    p.createDirectory(createParents = true, failIfExists = false)
    p / c
  }

  def get(id: Identifier[ManagedPath]): Option[Path] = {
    val dirStructure(a, b, c) = id.s
    for(s <- rootPath.segments.tail) { require(!s.contains("/")) }
    require(!a.contains("/"))
    require(!b.contains("/"))
    require(!c.contains("/"))
    
    val p: Path = rootPath / a / b / c
    if (p.exists) {
      for(s <- p.segments.tail) { require(!s.contains("/")) }
      Some(p)
    } else None
  }

  def exists(id: Identifier[ManagedPath]): Boolean = {
    val dirStructure(a, b, c) = id.s
    val p: Path = rootPath / a / b / c
    p.exists
  }


  def cleanup() = {
    for (a <- rootPath.children().toIterator if a.isDirectory;
         b <- a.children().toIterator) {
      if (b.children().isEmpty) {
        b.delete()
      }
    }
    for (a <- rootPath.children().toIterator if a.isDirectory) {
      if (a.children().isEmpty) {
        a.delete()
      }
    }
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
