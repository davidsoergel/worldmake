package worldmake.lib

import worldmake.{IdentifiableFunction2, IdentifiableFunction1}
import scalax.file.Path
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
object FileDerivations extends Logging {

  val countLines = new IdentifiableFunction1[Path, Integer]("countLines", (path: Path) => {
    logger.debug("Counting lines in " + path.fileOption.get.toString)
    val lines: Seq[String] = Source.fromFile(path.fileOption.get)(scala.io.Codec.UTF8).getLines().toSeq
    val result = lines.length
    logger.debug(s"Found $result lines")
    result
  })

}
