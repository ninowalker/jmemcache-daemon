package com.thimbleware.jmemcached.ws.resources

import collection.mutable.ArrayBuffer
import com.google.common.collect.Sets
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import javax.ws.rs._
import org.jboss.resteasy.spi.NotFoundException
import com.thimbleware.jmemcached.{MemCacheDaemon, Cache, MCElement}
import com.thimbleware.jmemcached.ws.StorageType
import com.thimbleware.jmemcached.storage.bytebuffer.{ByteBufferBlockStore, ByteBufferCacheStorage}
import com.thimbleware.jmemcached.storage.CacheStorage
import com.thimbleware.jmemcached.storage.hash.LRUCacheStorageDelegate
import com.thimbleware.jmemcached.storage.mmap.MemoryMappedBlockStore
import scala.collection.JavaConversions._
import xml.Elem

class WSApplication extends javax.ws.rs.core.Application {
    def getClasses = Sets.newHashSet();

    // the list of resources we provide
    override def getSingletons = Sets.newHashSet(InstancesResource);
}


/**
 * Top level list of instances resource. Knows about all the instances, and knows how to find them.
 */
@Path("/instances")
object InstancesResource {

    val instances = ArrayBuffer[InstanceResource]()

    /**
     * Return all the resources
     */
    @GET
    @Produces(Array("text/xml"))
    def all =
        <instances>
            {for (i <- instances) yield i.get}
        </instances>

    /**
     * Return a specific resource
     */
    @Path("/{id}")
    @Produces(Array("text/xml"))
    def instance(@PathParam("id") id:Int) : InstanceResource =
        instances.find( (instance) => instance.id == id) match {
            case Some(i:InstanceResource) => i
            case None => throw new NotFoundException("instance (" + id + ") not found");
        }


    @POST
    @Consumes(Array("application/x-www-form-urlencoded", "multipart/form-data"))
    def addInstance(
            @FormParam("listenHost") listenHost:String,
            @FormParam("port") port:Int,
            @FormParam("maxItemsSize") maxItemsSize: Int,
            @FormParam("maxMemorySize") maxMemorySize : Long,
            @FormParam("storageType") storageType : StorageType,
            @FormParam("blockSize") blockSize : Integer,
            @FormParam("memoryMappedFile") memoryMappedFile : String,
            @FormParam("ceilingBytes") ceilingBytes : Long) : Unit = {

        val instanceResource: InstanceResource = InstanceResource(
            id = instances.length,
            listenHost = listenHost,
            port = port,
            maxItemsSize = maxItemsSize,
            maxMemorySize = maxMemorySize,
            binaryProtocol = false,
            storageType = storageType,
            memoryMappedFile = memoryMappedFile,
            blockSize = if (blockSize == null) 8 else blockSize.intValue,
            ceilingBytes = ceilingBytes)
        instanceResource.createDaemon
        instanceResource.startDaemon
        instances += instanceResource
    }



}



/**
 * Represents a given instance.
 */
case class InstanceResource(id:Int,
                            listenHost:String,
                            port:Int,
                            maxItemsSize: Int,
                            maxMemorySize : Long,
                            ceilingBytes : Long,
                            binaryProtocol : Boolean,
                            storageType : StorageType,
                            memoryMappedFile : String,
                            blockSize : Int) {

    var daemon : MemCacheDaemon = null;

    def createDaemon() = {
        val cacheStorage : CacheStorage =
        if (storageType.isBlockStore) new ByteBufferCacheStorage(
            if (storageType == StorageType.MMAPPED_BLOCK) new MemoryMappedBlockStore(maxMemorySize, memoryMappedFile, blockSize)
            else new ByteBufferBlockStore(ByteBuffer.allocate(maxMemorySize.toInt), blockSize), maxItemsSize, ceilingBytes)
        else new LRUCacheStorageDelegate(maxItemsSize, maxMemorySize, ceilingBytes);

        daemon = new MemCacheDaemon(new Cache(cacheStorage));
        daemon.setBinary(binaryProtocol);
        daemon.setAddr(new InetSocketAddress(listenHost, port));
        daemon
    }

    def startDaemon = daemon.start
    def stopDaemon = daemon.stop

    @GET
    @Produces(Array("text/xml"))
    @Path("/")
    def get = <instance id={id.toString} running={daemon.isRunning.toString}>{binding}{stats}</instance>

    @GET
    @Produces(Array("text/xml"))
    @Path("/binding")
    def binding = <binding listeningHost={listenHost} port={port.toString}/>

    @GET
    @Produces(Array("text/xml"))    
    @Path("/stats")
    def stats = {
        val stats : java.util.Map[String, java.util.Set[String]] = daemon.getCache.stat("")

        // have to do this the old fashioned way because the conversion from the java map is failing
        var statElements = ArrayBuffer[Elem]();
        for (key <- stats.keySet ) {
            for (value <- stats.get(key)) {
                statElements += <stat key={key} value={value}/>
            }
        }

        <stats>{for (el <- statElements) yield el;}</stats>
    }

    @GET
    @Produces(Array("text/xml"))
    @Path("/items")
    def items() {
        val keys = daemon.getCache.stat("keys");

        // have to do this the old fashioned way because the conversion from the java map is failing
        var keyElements = ArrayBuffer[Elem]();
        for (key <- keys.keySet ) {
            for (value <- keys.get(key)) {
                keyElements += <item value={value}/>
            }
        }

        <items>{for (el <- keyElements) yield el;}</items>
    }


    @GET
    @Produces(Array("text/xml"))
    @Path("/items/{key}")
    def item(@PathParam("key") key : String) {
        val entry : Array[MCElement] = daemon.getCache.get(key);
        if (entry.size == 0) throw new NotFoundException("key (" + key + ") not found");
        val element : MCElement = entry(0);
        if (element == null || element.keystring == null) throw new NotFoundException("key (" + key + ") not found");
        <item key={element.keystring.toString} cas_unique={element.cas_unique.toString} length={element.dataLength.toString}
                 expire={element.expire.toString}/>
    }

    @GET
    @Produces(Array("text/xml"))
    @Path("/items/{key}/value")
    def itemValue(@PathParam("key") key : String) = {
        val entry : Array[MCElement] = daemon.getCache.get(key);
        if (entry.size == 0) throw new NotFoundException("key (" + key + ") not found");
        val element : MCElement = entry(0);
        if (element == null || element.keystring == null) throw new NotFoundException("key (" + key + ") not found");
        <item key={element.keystring.toString} cas_unique={element.cas_unique.toString} length={element.dataLength.toString}
               expire={element.expire.toString}>{new String(element.data)}</item>
    }
}
