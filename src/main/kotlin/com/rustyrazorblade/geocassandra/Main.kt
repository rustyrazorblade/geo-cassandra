package com.rustyrazorblade.geocassandra
import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter

class Args {
    @Parameter(names = arrayOf("-c", "--cluster"),
               description = "Cassandra cluster seeds (comma separated for multiple)",
               required = true)
    var cluster = String()
}

fun main(args: Array<String>) {
    var parsed = JCommander.newBuilder().addObject(Args()).build().parse(*args)


    println("Starting up the fun")

}