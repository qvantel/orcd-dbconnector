package se.qvantel.connector
import property.{CountryCodes, GraphiteConfig, Logger, ProcessingConfig}

import scala.util.{Failure, Success}

object DBConnector extends CountryCodes with Logger
  with ProcessingConfig with SyncManager with GraphiteConfig {

  def main(args: Array[String]): Unit = {
    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()

    val dispatcher = new DatapointDispatcher()

    // checks if the user is running the sync or not.
    syncStarter(args, dispatcher)

    // Close UDP Connection
    dispatcher.close()

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
    // Attempt Connection to Carbon
    dispatcher.init(graphiteHost, graphitePort) match {
      case Success(_) => syncLoop(dispatcher, benchmark)
      case Failure(e) => logger.info(Console.RED + "Failed to setup UDP socket for Carbon, Error: " + e.toString + Console.RESET)
    }
  }
}
