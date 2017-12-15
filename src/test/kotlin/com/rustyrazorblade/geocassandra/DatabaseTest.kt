package com.rustyrazorblade.geocassandra

import junit.framework.TestCase
import org.junit.Rule
import org.junit.Test

class DatabaseTest : TestCase() {
    var db: Database = Database("127.0.0.1", "geo")

    init {
        db.prepare_all()
    }

    @Test
    fun testUpdateLocationThenSearch() {
        db.updateDeviceLocation("test", 30.0, 60.0)
        db.updateDeviceLocation("test2", 30.00001, 60.0000)
        db.updateDeviceLocation("test3", 30.00000, 60.001)

        // should return test2
        var result = db.findNearbyDevices(30.0, 60.0, .1)

        // 3 results exepcted
    }

    @Test
    fun testIgnore() {
        db.ignoreDevice("test", "test2")

        val results = db.getIgnored("test")

        assertEquals(1, results.count())
    }
}