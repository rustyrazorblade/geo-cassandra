package com.rustyrazorblade.geocassandra

import com.datastax.driver.core.Session
import java.sql.PreparedStatement

/*
create table locations ( geohash text, device text, lat float, long float, primary key (geohash, device ) );
table of location updates
device could be a person or phone, whatever, any text field
saving the original lat/long to rank within geohash
 */
class Location(var session: Session) {
    var queries = {"insert into locations (geohash, device, lat, long) values (?, ?, ?, ?)"}

}

enum class LocationQuery {
    INSERT
}