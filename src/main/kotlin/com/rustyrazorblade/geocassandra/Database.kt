package com.rustyrazorblade.geocassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session

import org.slf4j.LoggerFactory
import com.github.davidmoten.geo.GeoHash
import org.locationtech.spatial4j.context.SpatialContext
import java.util.*
import kotlin.coroutines.experimental.buildIterator

/*

CREATE TABLE device (device text primary key, bloom_filter int)
 WITH compaction = {'class':'LeveledCompactionStrategy'}
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
 AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};


CREATE TABLE device_hide (device text, other text, primary key(device, other))
  WITH compaction = {'class':'LeveledCompactionStrategy'}
  AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
  AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};

create table location_updates ( geohash text, device text, lat double, long double, primary key (geohash, device ) );

 */
class Database(contact: String, keyspace: String) {

    var hashLength = 6
    var cluster: Cluster
    var session: Session
    var logger = LoggerFactory.getLogger(this::class.java)

    val statements = mapOf(
            Query.INSERT_LOCATION to "INSERT INTO location_updates (geohash, device, lat, long) values (?,?,?,?) USING TTL 3600",
            Query.SELECT_DEVICES_BY_LOCATION to "SELECT geohash, device, lat, long FROM location_updates WHERE geohash = ?"
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

    // no user, just a generic search for stuff near a point
    // TODO: Figure out a reasonable distance.  Maybe 10Km?
    fun findNearbyDevices(lat: Double, long: Double) {
        findNearbyDevices(Optional.empty(), lat, long, Optional.empty())
    }

    /*
     returns up to 50 close results
     first it checks the geohash
     next the neighboring ones
     then finally draws a bounding box


     don't want to return the device
      */
    fun findNearbyDevices(device: Optional<String>, lat: Double, long: Double, distance: Optional<Double>) : List<Device> {

        val result = mutableListOf<Device>()
        var executed = 0

        var seen = mutableSetOf<String>()
        // first look up the exact hash given the coordinates

        val hash = GeoHash.encodeHash(lat, long, hashLength)
        seen.add(hash)

        executed++
        var devices = findByHash(hash)

        val num = devices.count()

        result.addAll(devices)

        if(result.count() > 50) {
            return result
        }

        // then go to neighbors
        val neighbors = GeoHash.neighbours(hash)
        for(hash in neighbors) {

        }

        val point = geo.shapeFactory.pointXY(lat, long)

        val rect = geo.distCalc.calcBoxByDistFromPt(point, distance.orElse(0.1), geo, null)
        logger.info("Rectangle: $rect")
        // fetch all geo codes within the bounding box
        var hashes = GeoHash.coverBoundingBox(rect.maxX, rect.maxY, rect.minX, rect.minY, 6)
        logger.info("Hahes: $hashes")

        // then use the entire bounding box

        for(hash in hashes.hashes.shuffled()) {
            executed++
            devices = findByHash(hash)
            result.addAll(devices)
        }
        logger.info("$executed queries executed, ${result.count()} found")

        return result
    }

    /*
    Internal call for findNearbyDevices
     */
    private fun findByHash(hash: String): List<Device> {
        val query = queries.get(Query.SELECT_DEVICES_BY_LOCATION)!!

        val bound = query.bind(hash)

        logger.info("Pulling back exact hash match")
        val data = session.execute(bound)

        val devices = data.map { Device(device = it.getString("device") ) }
        return devices
    }



}

enum class Query {
    INSERT_LOCATION, SELECT_DEVICES_BY_LOCATION,

}

/*
table of location updates
device could be a person or phone, whatever, any text field
saving the original lat/long to rank within geohash
*/

data class Location(val geohash: String, val device: String, val lat: Float)

data class Device(val device: String)
