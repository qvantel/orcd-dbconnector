import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import com.sun.jmx.snmp.Timestamp
import property.CountryCodes
import property.Logger

import scala.util.{Failure, Success, Try}
import scala.util.{Failure, Success}

case class Model(id: Int, ts: DateTime)
object DBConnector extends SparkConnection with CountryCodes with Logger {

  def main(args: Array[String]): Unit = {

    //
    getCountriesByMcc()

    // Graphite connection info
    val graphiteIP = "localhost"
    val graphitePort = 2003

    // variable for the latest sync time
    var latestSyncDate: Long = 0

    var syncRdd = context.cassandraTable("qvantel", "latestsync")


    // Create dispatcher
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

    var latestSyncDate: Long = 0
    var syncRdd = context.cassandraTable("qvantel", "latestsync")
    //println(syncRdd.first())

    // latest sync time
    if(syncRdd.count() > 0) {
      syncRdd.select("id", "ts")
        .where("id = ?", 1)
        .collect()
        .foreach(row => {
          // assign the timestamp
          latestSyncDate = row.getLong("ts")
        })
    }

    // Cassandra table context
    val rdd = context.cassandraTable("qvantel", "call")


    // Update interval and batchSize setup config
    var lastUpdate = new DateTime(latestSyncDate)
    val updateInterval = 2000
    val batchSize = 250

    logger.info("Entering sync loop")
    // Syncing loop
    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = lastUpdate.getMillis() + updateInterval - DateTime.now(DateTimeZone.UTC).getMillis()
      if (sleepTime >= 0) {
        Thread.sleep(sleepTime)
      }

      logger.info(s"Syncing since $lastUpdate")

      // Reset loop variables
      var msgCount = 0
      val fetchBatchSize = 10000
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)

      val select = Try {
        rdd.select("created_at", "event_details", "service", "used_service_units")
          .where("created_at > ?", timeLimit.toString()).withAscOrder
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          // Select service
          val service: String = row.getString("service")

          // Select created_at timestamp
          val timeStamp = row.getDateTime("created_at")

          // Select event_details
          val eventDetails = row.getUDTValue("event_details")


          // Select a_party country
          val APartyLocation = eventDetails.getUDTValue("a_party_location")
          val destination = APartyLocation.getString("destination")
          val countryCode = destination.substring(0, 3)
          // Get MCC country code
          val countryISO = countries(countryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)

          // Select used_service_units
          val usedServiceUnits = row.getUDTValue("used_service_units")
          val amount = usedServiceUnits.getInt("amount")

          // Add datapoint to dispatcher
          dispatcher.append(s"qvantel.cdr.$service.destination.$countryISO", amount.toString, timeStamp)
          lastUpdate = timeStamp


        })
        dispatcher.append(s"qvantel.dbconnector.throughput", msgCount.toString, new DateTime(DateTimeZone.UTC))
        dispatcher.dispatch()
        logger.info(s"Sent a total of $msgCount datapoints to carbon this iteration")

        // Insert current time stamp for syncing here.
        // Insert timestamp always on id=1 to only have one record of a timestamp.
        val date = DateTime.now()
        val collection = context.parallelize(Seq(Model(1,date)))
        collection.saveToCassandra("qvantel", "latestsync", SomeColumns("id","ts"))

      }
      select match {
        case Success(e) => true
        case Failure(e) => e.printStackTrace()
      }
    }
  }
}
