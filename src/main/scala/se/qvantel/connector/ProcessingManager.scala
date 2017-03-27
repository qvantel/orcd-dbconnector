package se.qvantel.connector
import com.datastax.spark.connector._
import org.joda.time.{DateTime, DateTimeZone}
import se.qvantel.connector.DBConnector._
import scala.util.{Failure, Random, Success, Try}

class ProcessingManager {

  def callProcessing(dispatcher: DatapointDispatcher): Unit = {

    val callRdd = context.cassandraTable("qvantel", "call")
    val callSync = context.cassandraTable("qvantel", "callsync")
    val latestSyncDate = getLatestSyncDate(callSync)
    var lastUpdate = new DateTime (latestSyncDate)

    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = lastUpdate.getMillis() + updateInterval - DateTime.now(DateTimeZone.UTC).getMillis()
      if (sleepTime >= 0){
        Thread.sleep(sleepTime)
      }

      logger.info(s"Syncing CALLS since $lastUpdate")

      // Reset loop variables
      var msgCount = 0
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)
      val startTime = System.nanoTime()
      val callFetch = Try {
        callRdd.select("created_at", "event_details", "service", "used_service_units")
          .where("created_at > ?", timeLimit.toString()).withAscOrder
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          val service = row.getString("service")
          val timeStamp = row.getDateTime("created_at")
          val eventDetails = row.getUDTValue("event_details")
          val isRoaming = eventDetails.getBoolean("is_roaming")

          // Select a_party country
          val aPartyLocation = eventDetails.getUDTValue("a_party_location")
          val aPartyDestination = aPartyLocation.getString("destination")
          val aPartyCountryCode = aPartyDestination.substring(0, 3)
          val aPartyCountryISO = countries(aPartyCountryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)

          // Select b_party country
          val bPartyLocation = eventDetails.getUDTValue("b_party_location")
          val bPartyDestination = bPartyLocation.getString("destination")
          val bPartyCountryCode = bPartyDestination.substring(0, 3)
          val bPartyCountryISO = countries(bPartyCountryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)

          // Select used_service_units
          val usedServiceUnits = row.getUDTValue("used_service_units")
          val amount = usedServiceUnits.getInt("amount")

          // Add datapoint to dispatcher
          if (isRoaming) {
            dispatcher.append(s"qvantel.call.$service.destination.from.$aPartyCountryISO.to.$bPartyCountryISO", amount.toString, timeStamp)
          }
          lastUpdate = timeStamp
        })
      }

      callFetch match {
        case Success(_) if msgCount > 0  => {
          commitBatch(dispatcher, msgCount)
          updateLatestSync("callsync")
          val endTime = System.nanoTime()
          val throughput = measureDataSendPerSecond(startTime, endTime, msgCount)
          dispatcher.append(s"qvantel.dbconnector.throughput.call", throughput.toString, DateTime.now())
        }
        case Success(_) if msgCount == 0  => logger.info("Was not able to fetch any new CALL row from Cassandra")
        case Failure(e) => e.printStackTrace()
      }
    }
  }

  def productProcessing(dispatcher: DatapointDispatcher): Unit = {

    val productRdd = context.cassandraTable("qvantel", "product")
    val productSync = context.cassandraTable("qvantel", "productsync")
    val latestSyncDate = getLatestSyncDate(productSync)
    var lastUpdate = new DateTime(latestSyncDate)

    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = lastUpdate.getMillis() + updateInterval - DateTime.now(DateTimeZone.UTC).getMillis()
      if (sleepTime >= 0) {
        Thread.sleep(sleepTime)
      }

      logger.info(s"Syncing PRODUCT since $lastUpdate")

      // Reset loop variables
      var msgCount = 0
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)
      val startTime = System.nanoTime()
      val productFetch = Try {
        productRdd.select("created_at", "event_details", "service", "used_service_units", "event_charges")
          .where("created_at > ?", timeLimit.toString()).withAscOrder
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          val timeStamp = row.getDateTime("created_at")
          val eventCharges = row.getUDTValue("event_charges")
          val product = eventCharges.getUDTValue("product")
          val productName = product.getString("name").replaceAll("\\s+", "")

          val magicnumber = 1000
          dispatcher.append(s"qvantel.product.$productName", Random.nextInt(magicnumber).toString, timeStamp)
          lastUpdate = timeStamp
        })
      }

      productFetch match {
        case Success(_) if msgCount > 0 => {
          commitBatch(dispatcher, msgCount)
          updateLatestSync("productsync")
          val endTime = System.nanoTime()
          val throughput = measureDataSendPerSecond(startTime, endTime, msgCount)
          dispatcher.append(s"qvantel.dbconnector.throughput.product", throughput.toString, DateTime.now())
        }
        case Success(_) if msgCount == 0 => logger.info("Was not able to fetch any new PRODUCT row from Cassandra")
        case Failure(e) => e.printStackTrace()
      }
    }
  }

  private def measureDataSendPerSecond(startTime: Long, endTime: Long, msgCounter: Int): Double = {
    val nanosec = 1000000000
    val timedelta = (endTime - startTime) / nanosec
    var result = 0.0
    if (timedelta > 0) { // Handle possible division by zero
      result = msgCounter / timedelta
    }

    logger.info(result + " calls/second")
    result
  }
}
