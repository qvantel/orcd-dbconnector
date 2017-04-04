package se.qvantel.connector
import property.{CountryCodes, Logger, Processing}
import scala.util.{Failure, Success}

object DBConnector extends CountryCodes with Logger with Processing with SyncManager {

  def main(args: Array[String]): Unit = {
    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()

    val graphiteIP = "localhost"
    val graphitePort = 2003
    val dispatcher = new DatapointDispatcher()

    // checks if the user is running the sync or not.
    syncStarter(args, dispatcher, graphiteIP, graphitePort)

    // Close UDP Connection
    dispatcher.close()

    // Stop SparkContext
    context.stop()

    // Close cassandra session
    session.close()
  }

  def syncStarter(arg: Array[String], dispatcher: DatapointDispatcher, graphiteIP : String, graphitePort : Int): Unit = {
    var benchmark = false
    val benchmarkActivatingMsg = "benchmark is activated!"
    val errorArgsMsg = "the arguments were wrong, ->try --benchmark"
    val BenchmarkMsg = "--benchmark"

    if (arg.length > 0) {
      arg(0) match {
        case BenchmarkMsg =>
          logger.info(benchmarkActivatingMsg)
          benchmark = true
        case _ => logger.info(errorArgsMsg)
      }
    }
    // Attempt Connection to Carbon
    dispatcher.init(graphiteIP, graphitePort) match {
      case Success(_) => syncLoop(dispatcher, benchmark)
      case Failure(e) => logger.info(Console.RED + "Failed to setup UDP socket for Carbon, Error: " + e.toString + Console.RESET)
    }
  }
}
