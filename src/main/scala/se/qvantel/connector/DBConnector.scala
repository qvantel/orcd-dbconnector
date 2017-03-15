package se.qvantel.connector
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import property.{CountryCodes, Logger, Processing}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Random, Success, Try}

case class SyncModel(id: Int, ts: DateTime)

object DBConnector extends CountryCodes with Logger with Processing with SyncManager {

  def main(args: Array[String]): Unit = {

    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()

    val graphiteIP = "localhost"
    val graphitePort = 2003

    val dispatcher = new DatapointDispatcher(graphiteIP, graphitePort)

    // Attempt Connection to Carbon
    dispatcher.connect() match {
      case Success(_) => syncLoop(dispatcher)
      case Failure(e) => logger.info(Console.RED + "Failed to setup UDP socket for Carbon, Error: " + e.toString + Console.RESET)
    }


    // Close UDP Connection
    dispatcher.close()

    // Stop SparkContext
    context.stop()

    // Close cassandra session
    session.close()
  }

  def syncLoop(dispatcher: DatapointDispatcher): Unit = {
    logger.info("Starting processing of CALLS and PRODUCTS")
    val processmanage = new ProcessingManager()

    val startTime = System.nanoTime()
    val f1 = Future(processmanage.callProcessing(dispatcher))
    val f2 = Future(processmanage.productProcessing(dispatcher))
    val endTime = System.nanoTime()
    val differTime = endTime - startTime

   // Waiting for just one Future as there is no point running if either product or call fails
    Await.result(f1, Duration.Inf)
  }



  def commitBatch(dispatcher: DatapointDispatcher, msgCount: Int): Unit = {
    dispatcher.append(s"qvantel.dbconnector.throughput", msgCount.toString, new DateTime(DateTimeZone.UTC))
    dispatcher.dispatch()
    logger.info(s"Sent a total of $msgCount datapoints to carbon this iteration")
  }

}
