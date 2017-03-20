package se.qvantel.connector
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import property.{CountryCodes, Logger, Processing}
import scala.util.{Failure, Random, Success, Try}

case class SyncModel(id: Int, ts: DateTime)

object DBConnector extends CountryCodes with Logger with Processing with SyncManager {

  def main(args: Array[String]): Unit = {

    val argNr = parseArgument(args)

    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()

    val graphiteIP = "localhost"
    val graphitePort = 2003

    val dispatcher = new DatapointDispatcher(graphiteIP, graphitePort)

    // Attempt Connection to Carbon
    dispatcher.connect() match {
      case Success(_) => syncLoop(dispatcher, argNr)
      case Failure(e) => logger.info(Console.RED + "Failed to setup UDP socket for Carbon, Error: " + e.toString + Console.RESET)
    }

    // Close UDP Connection
    dispatcher.close()

    // Stop SparkContext
    context.stop()

    // Close cassandra session
    session.close()
  }


  def commitBatch(dispatcher: DatapointDispatcher, msgCount: Int): Unit = {
    dispatcher.append(s"qvantel.dbconnector.throughput", msgCount.toString, new DateTime(DateTimeZone.UTC))
    dispatcher.dispatch()
    logger.info(s"Sent a total of $msgCount datapoints to carbon this iteration")
  }


  def parseArgument(argArray: Array[String]): Int = {
    val nr = argArray(1).toInt
    nr
  }

}