package com.rustyrazorblade.geocassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session

import org.slf4j.LoggerFactory
import com.github.davidmoten.geo.GeoHash
import org.locationtech.spatial4j.context.SpatialContext
import java.util.*
import kotlin.coroutines.experimental.buildSequence

/*

CREATE TABLE device (device text primary key, bloom_filter int)
 WITH compaction = {'class':'LeveledCompactionStrategy'}
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
 AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};


CREATE TABLE device_ignore (device text, other text, primary key(device, other))
  WITH compaction = {'class':'LeveledCompactionStrategy'}
  AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
  AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};

create table location_updates ( geohash text, device text, lat double, long double, primary key (geohash, device ) );

CREATE table ignore_stats ( device text primary key, num counter )
  WITH compaction = {'class':'LeveledCompactionStrategy'}
  AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
  AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};

 */
class Database(contact: String, keyspace: String) {

    var hashLength = 6
    var cluster: Cluster
    var session: Session
    var logger = LoggerFactory.getLogger(this::class.java)

    val statements = mapOf(
            Query.INSERT_LOCATION to "INSERT INTO location_updates (geohash, device, lat, long) values (?,?,?,?) USING TTL 3600",
            Query.SELECT_DEVICES_BY_LOCATION to "SELECT geohash, device, lat, long FROM location_updates WHERE geohash = ?",
            Query.INSERT_IGNORE to "INSERT INTO device_ignore (device, other) VALUES (?, ?)",
            Query.SELECT_IGNORED to "SELECT other from device_ignore WHERE device = ?",
            Query.ADD_IGNORED to "UPDATE ignore_stats SET num = num + 1 WHERE device = ?"
    )

    var queries = mutableMapOf<Query, PreparedStatement>()
    var geo = SpatialContext.GEO

    init {
        logger.info("Connecting to cluster")
        this.cluster = Cluster.builder().addContactPoint(contact).build()
        this.session = cluster.connect(keyspace)
    }

    fun prepare_all() {
        for((key, query) in statements) {
            var prepared = session.prepare(query)
            queries[key] = prepared
        }
    }

    fun updateDeviceLocation(device: String, lat: Double, long: Double) {
        val hash = GeoHash.encodeHash(lat, long, hashLength)
        var bound = queries[Query.INSERT_LOCATION]!!.bind(hash, device, lat, long)
        session.execute(bound)
    }

    fun findNearbyDevices(lat: Double, long: Double, distance: Double): Sequence<Device> {
        return findNearbyDevices(Optional.empty(), lat, long, Optional.of(distance))
    }

    // no user, just a generic search for stuff near a point
    // TODO: Figure out a reasonable distance.  Maybe 10Km?
    fun findNearbyDevices(lat: Double, long: Double) : Sequence<Device> {
        return findNearbyDevices(Optional.empty(), lat, long, Optional.empty())
    }

    /*
     returns up to 50 close results
     first it checks the geohash
     next the neighboring ones
     then finally draws a bounding box


     don't want to return the device
      */
    fun findNearbyDevices(device: Optional<String>, lat: Double, long: Double, distance: Optional<Double>)  = buildSequence {

        val result = mutableListOf<Device>()
        var executed = 0

        var seen = mutableSetOf<String>()
        // first look up the exact hash given the coordinates
        val hash = GeoHash.encodeHash(lat, long, hashLength)
        seen.add(hash)

        executed++
        var devices = findByHash(listOf(hash))

        val num = devices.count()

        // first group of results
        yieldAll(devices)

        // then go to neighbors
        val neighbors = GeoHash.neighbours(hash)
        neighbors.removeAll(seen)

        // yield each of the neighbors info
        for (hash in neighbors) {
            // don't re-query for the hashes we've already seen
            if (seen.contains(hash))
                continue

            seen.add(hash)

            yieldAll(findByHash(listOf(hash)))
        }

        // we've now yielded the direct cell and the neighboring ones
        // lets draw a bounding box and yield the rest in random order

        val point = geo.shapeFactory.pointXY(lat, long)

        val rect = geo.distCalc.calcBoxByDistFromPt(point, distance.orElse(0.1), geo, null)
        logger.info("Rectangle: $rect")

        // fetch all geo codes within the bounding box
        var hashes = GeoHash.coverBoundingBox(rect.maxX, rect.maxY, rect.minX, rect.minY, 6).hashes.shuffled()

        var tmp = mutableListOf<String>()

        for (hash in hashes) {
            yieldAll(findByHash(listOf(hash)))
        }
    }

    /*
    Internal call for findNearbyDevices
    Accepts multiple hashes
    Will query for all of them in async form, merge, return
     */
    fun findByHash(hashes: List<String>) = buildSequence {
        val query = queries.get(Query.SELECT_DEVICES_BY_LOCATION)!!

        var result = mutableListOf<Device>()
        for(hash in hashes) {
            val bound = query.bind(hash)

            logger.debug("Pulling back hash $hash")

            val data = session.execute(bound)

            val devices = data.map { Device(device = it.getString("device")) }
            yieldAll(devices)
        }
    }

    // mark someone seen
    fun ignoreDevice(device: String, otherDevice: String) {
        val query = queries.get(Query.INSERT_IGNORE)!!
        val bound = query.bind(device, otherDevice)
        session.executeAsync(bound)

        val query2 = queries.get(Query.ADD_IGNORED)!!
        val bound2 = query2.bind(device)
        session.executeAsync(bound2)
    }

    fun getIgnored(device: String) : List<Device> {
        val query = queries.get(Query.SELECT_IGNORED)!!
        val bound = query.bind(device)
        return session.execute(bound).map { Device(it.getString("other")) }
    }


}

enum class Query {
    INSERT_LOCATION,
    SELECT_DEVICES_BY_LOCATION,
    INSERT_IGNORE,
    SELECT_IGNORED,
    ADD_IGNORED

}

/*
table of location updates
device could be a person or phone, whatever, any text field
saving the original lat/long to rank within geohash
*/

data class Location(val geohash: String, val device: String, val lat: Float)

data class Device(val device: String)
