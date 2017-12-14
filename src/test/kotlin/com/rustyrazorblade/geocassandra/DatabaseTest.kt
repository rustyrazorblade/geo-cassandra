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
    fun testUpdateLocation() {
        db.updateDeviceLocation("test", 60.0F, 60.0F)
        db.updateDeviceLocation("test2", 60.0F, 60.0F)
    }

}