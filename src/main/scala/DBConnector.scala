import com.typesafe.scalalogging.Logger
import java.net._
import java.io._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._

import scala.util.{Failure, Success, Try}

object DBConnector extends SparkConnection {
  // Set up logging
  val logger = Logger("DBConnector")

  def main(args: Array[String]): Unit = {

    // Connection info
    val graphiteIP = "localhost"
    val graphitePort = 2003
    val graphiteAddress = new InetSocketAddress(graphiteIP, graphitePort)

    // Create and connect to socket
    val socket = new Socket()

    val socketSetup = Try {
      val timeout = 5000
      socket.connect(graphiteAddress, timeout)
    }

    socketSetup match {
      case Success(_) => syncLoop(socket)
      case Failure(e) => logger.info(e.toString)
    }

    // Close socket
    socket.close()

    // Close cassandra session
    session.close()
  }

  def syncLoop(socket: Socket): Unit = {
    // Cassandra table context
    val rdd = context.cassandraTable("database", "cdr")
    // Socket output stream
    val out = new PrintStream(socket.getOutputStream)

    // Update interval and batchSize setup config
    var lastUpdate = new DateTime(0)
    val updateInterval = 2000
    val batchSize = 250

    // Syncing loop
    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = lastUpdate.getMillis() + updateInterval - DateTime.now(DateTimeZone.UTC).getMillis()
      if (sleepTime >= 0) {
        Thread.sleep(sleepTime)
      }

      logger.info(s"Syncing since $lastUpdate")

      // Reset loop variables
      var payload = ""
      var msgCount = 0
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)

      // Fetch data since last update
      rdd.select("ts", "key", "value").where("ts > ?", timeLimit.toString()).collect().foreach(row => {
        msgCount += 1

        // Select data
        val value = row.getInt("value")
        val ts = (row.getDateTime("ts").getMillis() / 1000L)
        // Create carbon entry and append to payload
        payload += s"database.cdr.value $value $ts\n"

        // Send events in batches of 250
        if (msgCount % batchSize == 0) {
          logger.info(s"Sending $batchSize datapoints to carbon")
          out.print(payload)
          payload = ""
        }
      })

      // Send the last remaining events
      val eventsLeft = msgCount % batchSize
      logger.info(s"Sending $eventsLeft datapoints to carbon")
      out.print(payload)

      logger.info(s"Sent a total of $msgCount datapoints to carbon this iteration")
    }

  }
}
