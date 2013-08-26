package worldmake

import org.joda.time.DateTime

// todo this is just a feature request stub

/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */
trait ProvenanceAnnotation {

  def createdTime: DateTime

  def regarding: Set[Provenance[_]]

  def author: Option[String]

  // Person...
  def notes: Option[String]
}
