package se.qvantel.connector

import kamon.Kamon
import property.{CountryCodes, GraphiteConfig, Logger, ProcessingConfig}

import scala.util.{Failure, Success, Try}

object DBConnector extends CountryCodes with Logger
  with ProcessingConfig with SyncManager with GraphiteConfig {

  def main(args: Array[String]): Unit = {

    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()

    // Make sure Kamon starts
    Try(Kamon.start()) match {
      case Success(_) => {
        logger.info("Kamon started successfully")
        syncStarter(args)
      }
      case Failure(e) => logger.error(e.toString)
    }

    // Stop SparkContext
    context.stop()

    // Close cassandra session
    session.close()

    // Close Kamon session
    Kamon.shutdown()
  }

  def syncStarter(arg: Array[String]): Unit = {
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
    syncLoop(benchmark)
  }
}
