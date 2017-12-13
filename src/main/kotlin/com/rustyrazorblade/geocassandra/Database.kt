package com.rustyrazorblade.geocassandra

import ch.qos.logback.classic.Logger
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.LoggerFactory


class Database(contact: String, keyspace: String) {

    var cluster: Cluster
    var session: Session
    var logger = LoggerFactory.getLogger(this::class.java)

    init {
        logger.info("Connecting to cluster")
        this.cluster = Cluster.builder().addContactPoint(contact).build()
        this.session = cluster.connect(keyspace)
    }

}