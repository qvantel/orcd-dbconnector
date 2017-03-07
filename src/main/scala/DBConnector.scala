import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import property.CountryCodes
import property.Logger

import scala.util.{Failure, Success, Try}

case class Model(id: Int, ts: DateTime)

object DBConnector extends SparkConnection with CountryCodes with Logger {

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

    val latestSyncDate = getLatestSyncDate()
    val rdd = context.cassandraTable("qvantel", "call")

    // Update interval and batchSize setup config
    var lastUpdate = new DateTime(latestSyncDate)
    val updateInterval = 2000
    val batchSize = 250
    val fetchBatchSize = 10000

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
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)

      val select = Try {
        rdd.select("created_at", "event_details", "service", "used_service_units")
          .where("created_at > ?", timeLimit.toString()).withAscOrder
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          val service  = row.getString("service")
          val timeStamp = row.getDateTime("created_at")
          val eventDetails = row.getUDTValue("event_details")

          // Select a_party country
          val aPartyLocation = eventDetails.getUDTValue("a_party_location")
          val destination = aPartyLocation.getString("destination")
          val countryCode = destination.substring(0, 3)
          val countryISO = countries(countryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)

          // Select used_service_units
          val usedServiceUnits = row.getUDTValue("used_service_units")
          val amount = usedServiceUnits.getInt("amount")

          // Add datapoint to dispatcher
          dispatcher.append(s"qvantel.call.$service.destination.$countryISO", amount.toString, timeStamp)
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
        case Success(e) => None
        case Failure(e) => e.printStackTrace()
      }
    }
  }

  def getLatestSyncDate(): Long = {
    val syncRdd = context.cassandraTable("qvantel", "latestsync")

    if (syncRdd.count() > 0) {
      syncRdd.first().get[Long]("ts")
    } else {
      0 // sync time will be set to POSIX time
    }
  }

}
