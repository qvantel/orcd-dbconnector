package se.qvantel.connector

import com.typesafe.scalalogging.LazyLogging
import property.{CountryCodes, GraphiteConfig, ProcessingConfig}
import scala.util.{Failure, Success}

object DBConnector extends CountryCodes with LazyLogging
  with ProcessingConfig with SyncManager with GraphiteConfig {

  def main(args: Array[String]): Unit = {
    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()

    val dispatcher = new DatapointDispatcher()

    // checks if the user is running the sync or not.
    syncStarter(args, dispatcher)

    // Stop SparkContext
    context.stop()

    // Close cassandra session
    session.close()
  }

  def syncStarter(arg: Array[String], dispatcher: DatapointDispatcher): Unit = {
    var benchmark = false
    val benchmarkMsg = "--benchmark"

    if (arg.length > 0) {
      arg(0) match {
        case `benchmarkMsg` => {
          val benchmarkActivatingMsg = "benchmark is activated!"
          logger.info(benchmarkActivatingMsg)
          benchmark = true
        }
        case _ => {
          val errorArgsMsg = "the arguments were wrong, ->try --benchmark"
          logger.info(errorArgsMsg)
        }
      }
    }

    // Check if connection to Carbon is possible
    dispatcher.connect match {
      case Some(s) => {
        s.close()
        syncLoop(dispatcher, benchmark)
      }
      case None => logger.error(connectionFailureMsg)
    }
  }
}
