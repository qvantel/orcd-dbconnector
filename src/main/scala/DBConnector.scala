import org.apache.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.scalalogging.Logger
import java.net._
import java.io._
import scala.io._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

object DBConnector {

  def main(args: Array[String]): Unit =
  {
    // Set up logging
    val logger = Logger("DBConnector")


    // Configure spark->cassandra connection
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
    val context = new SparkContext("local[2]", "database", conf)

    // Setup cassandra connector
    val connector = CassandraConnector(conf)
    // Create cassandra session
    val session = connector.openSession()
    // Cassandra table context
    val rdd = context.cassandraTable("database", "cdr")

    // Connection info
    val graphiteIP = "localhost"
    val graphitePort = 2003
    val graphiteAddress = new InetSocketAddress(graphiteIP, graphitePort)

    var lastUpdate = new DateTime(0)
    val updateInterval = 2000
    val batchSize = 250

    // Create and connect to socket
    val socket = new Socket()
    try {
      val timeout = 5000
      socket.connect(graphiteAddress, timeout)
    }
    catch {
      case se: SocketException => {
        logger.error("UDP Socket unable to connect")
        System.exit(1)
      }
      case e: Exception => {
        logger.error("Unknown error occured during UDP socket connection")
        System.exit(1)
      }
    }
    // Socket output stream
    val out = new PrintStream(socket.getOutputStream)

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
        if (msgCount % batchSize == 0){
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
    socket.close()

    // Close cassandra session
    session.close()
  }
}
