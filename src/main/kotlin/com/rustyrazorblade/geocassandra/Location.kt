package com.rustyrazorblade.geocassandra

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.sun.javafx.binding.Logging
import org.slf4j.LoggerFactory
import java.util.*

/*
create table locations ( geohash text, device text, lat float, long float, primary key (geohash, device ) );
table of location updates
device could be a person or phone, whatever, any text field
saving the original lat/long to rank within geohash
 */
class Location(var session: Session) {

    val queries = mapOf(LocationQuery.INSERT
                         to "INSERT INTO location_updates (geohash, device, lat, long) values (?,?,?,?)")

    var prepared = mutableMapOf<LocationQuery, PreparedStatement>()

    var logger = LoggerFactory.getLogger("location")

    fun prepare_all() {
        for((key, query) in queries) {
            var string_query = queries[key]
            logger.info("Preparing: $query")
            var stmt = session.prepare(query)
            prepared[key] = stmt
        }
    }
}

enum class LocationQuery {
    INSERT
}


// quick test to see if locally prepared statements are ok
fun main(args: Array<String>) {
    var database = Database("127.0.0.1", "geo")
    var location = Location(database.session)
    location.prepare_all()
}