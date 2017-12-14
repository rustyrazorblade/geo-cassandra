package com.rustyrazorblade.geocassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session

import org.slf4j.LoggerFactory

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
                            to "INSERT INTO location_updates (geohash, device, lat, long) values (?,?,?,?)")

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


