import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import property.CountryCodes
import property.Logger

object DBConnector extends SparkConnection with CountryCodes with Logger {
  def main(args: Array[String]): Unit = {

    //
    getCountriesByMcc()

    // Graphite connection info
    val graphiteIP = "localhost"
    val graphitePort = 2003

    // Create dispatcher
    val dispatcher = new DatapointDispatcher(graphiteIP, graphitePort)
    dispatcher.connect()

    syncLoop(dispatcher)

    // Close UDP Connection
    dispatcher.close()

    // Close cassandra session
    session.close()
  }

  def syncLoop(dispatcher: DatapointDispatcher): Unit = {
    // Cassandra table context
    val rdd = context.cassandraTable("qvantel", "call")

    // Update interval and batchSize setup config
    var lastUpdate = new DateTime(0)
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
      var payload = ""
      var msgCount = 0
      val fetchBatchSize = 10000
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)

      rdd.select("created_at", "event_details", "service", "used_service_units")
        .where("created_at > ?", timeLimit.toString()).withAscOrder
        .limit(fetchBatchSize).collect().foreach(row => {

        msgCount += 1

        // Select service
        val service : String = row.getString("service")

        // Select created_at timestamp
        val timeStamp = row.getDateTime("created_at")

        // Select event_details
        val eventDetails: UDTValue = row.getUDTValue("event_details")

        // Select a_party country
        val APartyLocation: UDTValue = eventDetails.getUDTValue("a_party_location")
        val destination: String = APartyLocation.getString("destination")
        val countryCode : String = destination.substring(0, 3) // Get MCC country code
        val countryISO : String = countries(countryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)

        // Select used_service_units
        val usedServiceUnits : UDTValue = row.getUDTValue("used_service_units")
        val amount : Int = usedServiceUnits.getInt("amount")

        // Add datapoint to dispatcher
        dispatcher.append(s"qvantel.$service.destination.$countryISO", amount.toString, timeStamp)
        lastUpdate = timeStamp
      })
      dispatcher.dispatch()
      logger.info(s"Sent a total of $msgCount datapoints to carbon this iteration")
    }
  }
}
