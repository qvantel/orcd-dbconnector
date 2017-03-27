package se.qvantel.connector
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.Any
import property.{CountryCodes, Logger, Processing}

import scala.util.{Failure, Random, Success, Try}

case class SyncModel(id: Int, ts: DateTime)

object DBConnector extends CountryCodes with Logger with Processing with SyncManager {

  def main(args: Array[String]): Unit = {


    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()

    val graphiteIP = "localhost"
    val graphitePort = 2003

    val dispatcher = new DatapointDispatcher(graphiteIP, graphitePort)

    // checks if the user is running the benchmark or not
    benchmarkChecker(args, dispatcher)

    // Close UDP Connection
    dispatcher.close()

    // Stop SparkContext
    context.stop()

    // Close cassandra session
    session.close()
  }


  def commitBatch(dispatcher: DatapointDispatcher, msgCount: Int): Unit = {
    dispatcher.dispatch()
    logger.info(s"Sent a total of $msgCount datapoints to carbon this iteration")
  }


  def benchmarkChecker(arg: Array[String], dispatcher: DatapointDispatcher): Unit ={
    if(arg.length > 0)
    {
      arg(0) match {
        case "--benchmark" => logger.info("benchmark is activated.")
          // Attempt Connection to Carbon
          dispatcher.connect() match {
            case Success(_) => syncLoop(dispatcher, 0)
            case Failure(e) => logger.info(Console.RED + "Failed to setup UDP socket for Carbon, Error: " + e.toString + Console.RESET)
          }
        case _ => logger.info("the arguments were wrong, ->try --benchmark")
      }
    }
    else {
      // Attempt Connection to Carbon
      dispatcher.connect() match {
        case Success(_) => syncLoop(dispatcher, 1)
        case Failure(e) => logger.info(Console.RED + "Failed to setup UDP socket for Carbon, Error: " + e.toString + Console.RESET)
      }

    }


  }


}
