package com.rustyrazorblade.geocassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session

import org.slf4j.LoggerFactory
import com.github.davidmoten.geo.GeoHash
/*

CREATE TABLE device (device text primary key, bloom_filter int)
 WITH compaction = {'class':'LeveledCompactionStrategy'}
 AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
 AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};


CREATE TABLE device_hide (device text, other text, primary key(device, other))
  WITH compaction = {'class':'LeveledCompactionStrategy'}
  AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
  AND compression = {'class':'LZ4Compressor', 'chunk_length_kb':4};

create table locations ( geohash text, device text, lat float, long float, primary key (geohash, device ) );


 */
class Database(contact: String, keyspace: String) {

    var cluster: Cluster
    var session: Session
    var logger = LoggerFactory.getLogger(this::class.java)

    val statements = mapOf(Query.INSERT_LOCATION
                            to "INSERT INTO location_updates (geohash, device, lat, long) values (?,?,?,?) USING TTL 3600")

    var queries = mutableMapOf<Query, PreparedStatement>()

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

    fun updateDeviceLocation(device: String, lat: Float, long: Float) {
        val hash = GeoHash.encodeHash(lat.toDouble(), long.toDouble())
        var bound = queries[Query.INSERT_LOCATION]!!.bind(hash, device, lat, long)
        session.execute(bound)
    }

    // don't want to return the device
    fun findNearbyDevices(device: String, lat: Float, long: Float) {
        val hash = GeoHash.encodeHash(lat.toDouble(), long.toDouble())

        GeoHash.neighbours(hash)

    }

    fun distance(lat1: Double, lat2: Double, lon1: Double,
                 lon2: Double, el1: Double, el2: Double): Double {

        val R = 6371 // Radius of the earth

        val latDistance = Math.toRadians(lat2 - lat1)
        val lonDistance = Math.toRadians(lon2 - lon1)
        val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + (Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2))
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
        var distance = R.toDouble() * c * 1000.0 // convert to meters

        val height = el1 - el2

        distance = Math.pow(distance, 2.0) + Math.pow(height, 2.0)

        return Math.sqrt(distance)
    }

}

enum class Query {
    INSERT_LOCATION,

}

/*
table of location updates
device could be a person or phone, whatever, any text field
saving the original lat/long to rank within geohash
*/

data class Location(val geohash: String, val device: String, val lat: Float)


