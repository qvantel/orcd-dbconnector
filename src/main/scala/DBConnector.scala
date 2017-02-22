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

    val rdd = context.cassandraTable("database", "cdr")

    val graphite_port = 2003
    val socket = new Socket(InetAddress.getLocalHost(), graphite_port)
    lazy val in = new BufferedSource(socket.getInputStream()).getLines()
    val out = new PrintStream(socket.getOutputStream)

    var last_update = new DateTime(0)
    val update_interval = 2000

    while (true) {
      // Sleep $update_interval since last_update
      val sleeptime = last_update.getMillis()+update_interval-DateTime.now(DateTimeZone.UTC).getMillis()
      if (sleeptime >= 0)
        Thread.sleep(sleeptime)

      logger.info(s"Syncing since $last_update")

      var payload = ""
      var msgcount = 0
      val batchsize = 250
      val timelimit = last_update
      last_update = DateTime.now(DateTimeZone.UTC)

      // Fetch data since last update
      rdd.select("ts", "key", "value").where("ts > ?", timelimit.toString()).collect().foreach(row => {
        msgcount += 1

        // Select data
        val value = row.getInt("value")
        val ts = (row.getDateTime("ts").getMillis() / 1000L)
        // Create carbon entry and append to payload
        payload += s"database.cdr.value $value $ts\n"

        // Send events in batches of 250
        if (msgcount % batchsize == 0){
          logger.info(s"Sending $batchsize datapoints to carbon")
          out.print(payload)
          payload = ""
        }
      })

      // Send the last remaining events
      val events_left = msgcount % batchsize
      logger.info(s"Sending $events_left datapoints to carbon")
      out.print(payload)

      logger.info(s"Sent a total of $msgcount datapoints to carbon this iteration")
    }
    socket.close()

    // Close cassandra session
    session.close()
  }
}
