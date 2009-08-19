package com.thimbleware.jmemcached.ws.resources


import java.io.OutputStream
import java.lang.reflect.Type
import javax.ws.rs.core.{MultivaluedMap, MediaType}
import javax.ws.rs.ext.{Provider, MessageBodyWriter}
import xml.Elem

/**
 * Provider for JAX-RS to convert Scala xml Elem into Strings
 */
@Provider
class XMLBodyWriter extends MessageBodyWriter[Elem] {
    def getSize(t:Elem, typ:Class[_], genericType:Type, annotations:Array[java.lang.annotation.Annotation], mediaType:MediaType) : long =
        -1

    def isWriteable(typ: java.lang.Class[_],
                   genericType : java.lang.reflect.Type,
                   annotations : Array[java.lang.annotation.Annotation],
                   mediaType: javax.ws.rs.core.MediaType) : Boolean =
        mediaType.isCompatible(MediaType.APPLICATION_XML_TYPE) || mediaType.isCompatible(MediaType.TEXT_XML_TYPE);

    def writeTo(t:Elem, typ:Class[_], genericType:Type, annotations:Array[java.lang.annotation.Annotation], mediaType:MediaType,
            headers:MultivaluedMap[String, Object], entityStream:OutputStream) : Unit =
        entityStream.write(t.toString.getBytes)

}
