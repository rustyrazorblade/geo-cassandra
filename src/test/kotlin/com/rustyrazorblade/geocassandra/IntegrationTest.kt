package com.rustyrazorblade.geocassandra


import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import org.junit.Test


// Testing gRPC:
// https://github.com/grpc/grpc-java/blob/master/examples/src/test/java/io/grpc/examples/helloworld/HelloWorldClientTest.java

open class IntegrationTest {
    @get:Rule
    public val grpcServerRule = GrpcServerRule().directExecutor()

    @Test
    fun testSimple() {
        var db = Database("127.0.0.1", "geo")
        db.prepare_all()

        grpcServerRule.serviceRegistry.addService(GeoServer(db))
        /*
            GreeterGrpc.GreeterBlockingStub blockingStub = GreeterGrpc.newBlockingStub(grpcServerRule.getChannel());
            String testName = "test name";

            HelloReply reply = blockingStub.sayHello(HelloRequest.newBuilder().setName(testName).build());

            assertEquals("Hello " + testName, reply.getMessage());
         */
        var stub = GeoServiceGrpc.newBlockingStub(grpcServerRule.channel)


    }

}