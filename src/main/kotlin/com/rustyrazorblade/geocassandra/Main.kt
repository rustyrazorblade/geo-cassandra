package com.rustyrazorblade.geocassandra

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.rustyrazorblade.geocassandra.GeoServiceGrpc.GeoServiceImplBase
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory


class ParsedArgs {
    @Parameter(names = arrayOf("-c", "--cluster"),
               description = "Cassandra cluster seeds (comma separated for multiple)",
               required = true)
    var cluster = String()
}



fun main(args: Array<String>) {
    var logger = LoggerFactory.getLogger("main")
    logger.info("Starting up the fun")

    var parsedArgs = ParsedArgs()
    JCommander.newBuilder().addObject(parsedArgs).build().parse(*args)

    logger.info("Connecting to cluster ${parsedArgs.cluster}")
    var database = Database("127.0.0.1", "geo")
    database.prepare_all()

    var server = ServerBuilder.forPort(5000).addService(GeoServer(database)).build()
    server.awaitTermination()


}

// have to pass in the working cluster and connected session
class GeoServer(var database: Database) : GeoServiceImplBase() {

    override fun putUser(request: GeoCassandraServer.PutRequest?, responseObserver: StreamObserver<GeoCassandraServer.PutReply>?) {

        super.putUser(request, responseObserver)
    }

}


