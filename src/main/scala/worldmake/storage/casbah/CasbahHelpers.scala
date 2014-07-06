/*
 * Copyright (c) 2013  David Soergel  <dev@davidsoergel.com>
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package worldmake.storage.casbah

import collection.mutable
import com.mongodb.casbah.commons.Imports


import com.mongodb.casbah.Imports._

import com.mongodb.casbah.commons.conversions._
import com.mongodb.casbah.commons.conversions.scala._
import org.bson.{BSON, Transformer}
import java.net.URL


/**
 * @author <a href="mailto:dev@davidsoergel.com">David Soergel</a>
 */


private[casbah] trait MongoWrapper {
  def dbo: MongoDBObject

  def rawString = dbo.toString()
}

class IntegrityException(s: String) extends RuntimeException(s)

private[casbah] object SerializationHelpers {

  RegisterJodaTimeConversionHelpers()

  RegisterURLHelpers()

  /*
    implicit def uuidToEventProcessor(uuid : UUID): EventProcessor = eventProcessorStore.get(uuid)
      .getOrElse(throw new IntegrityException("Event Processor not found: " + uuid))
  
    def uuidToMutableMailingList(uuid : UUID): MutableMailingList =  uuidToEventProcessor(uuid) match {
      case mml : MutableMailingList  => mml
      case _ => throw new IntegrityException("EventProcessor is not mutable mailing list: " + uuid)
    }
  
    implicit def uuidToEvent(uuid : UUID): Event = eventStore.get(uuid)
      .getOrElse(throw new IntegrityException("Event not found: " + uuid))
  
    def uuidToPrimaryEvent(uuid : UUID): PrimaryEvent = uuidToEvent(uuid) match {
      case pe : PrimaryEvent => pe
      case _ => throw new IntegrityException("Event is not primary: " + uuid)
    }
  
    implicit def uuidToDocument(uuid : UUID): Document = documentStore.get(uuid)
      .getOrElse(throw new IntegrityException("Document not found: " + uuid))
  
    implicit def uuidToVenue(uuid : UUID): Venue = venueStore.get(uuid)
      .getOrElse(throw new IntegrityException("Venue not found: " + uuid))
  
  
    implicit def uuidToMessageGenerator(uuid : UUID): MessageGenerator = messageGeneratorStore.get(uuid)
      .getOrElse(throw new IntegrityException("Message generator not found: " + uuid))
      */
}

abstract class MongoSerializer[E, D](val typehint: String, constructor: MongoDBObject => D) {
  def toDb(e: E): D = {
    val builder = MongoDBObject.newBuilder
    builder += "type" -> typehint
    addFields(e, builder)
    val dbo = builder.result()
    constructor(dbo)
  }

  def addFields(e: E, builder: mutable.Builder[(String, Any), Imports.DBObject])
}


object RegisterURLHelpers extends URLSerializer with URLDeserializer {
  def apply() = {
    log.debug("Registering URL Serializers.")
    super.register()
  }
}


trait URLSerializer extends MongoConversionHelper {

  private val encodeType = classOf[URL]
  /** Encoding hook for MongoDB To be able to persist URL to MongoDB */
  private val transformer = new Transformer {

    def transform(o: AnyRef): AnyRef = {
      log.trace("Encoding a java.net.URL")
      o match {
        // TODO  There has to be a better way to marshall a URL than string munging like this...
        case url: java.net.URL => "URL~%s".format(url.toExternalForm)
        case _ => o
      }
    }
  }

  override def register() = {
    log.trace("Hooking up java.net.URL serializer.")
    BSON.addEncodingHook(encodeType, transformer)
    super.register()
  }

  override def unregister() = {
    log.trace("De-registering java.net.URL serializer.")
    BSON.removeEncodingHooks(encodeType)
    super.unregister()
  }
}

trait URLDeserializer extends MongoConversionHelper {
  private val encodeType = classOf[String]
  private val transformer = new Transformer {

    def transform(o: AnyRef): AnyRef = {
      log.trace("Decoding java.net.URL")
      o match {
        case s: String if s.startsWith("URL~") && s.split("~").size == 2 => {
          log.trace("DECODING: %s", s)
          new java.net.URL(s.split("~")(1))
        }
        case _ => o
      }
    }
  }

  override def register() = {
    log.trace("Hooking up java.net.URL deserializer")

    BSON.addDecodingHook(encodeType, transformer)
    super.register()
  }

  override def unregister() = {
    log.trace("De-registering java.net.URL  deserializer.")
    BSON.removeDecodingHooks(encodeType)
    super.unregister()
  }
}
