/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

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
