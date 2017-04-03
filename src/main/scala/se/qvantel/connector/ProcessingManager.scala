package se.qvantel.connector
import com.datastax.spark.connector._
import org.joda.time.{DateTime, DateTimeZone, Seconds}
import se.qvantel.connector.DBConnector._

import scala.util.{Failure, Success, Try}

class ProcessingManager {

  def cdrProcessing(dispatcher: DatapointDispatcher): Unit = {

    val cdrRdd = context.cassandraTable("qvantel", "cdr")
    val cdrSync = context.cassandraTable("qvantel", "cdrsync")
    val latestSyncDate = getLatestSyncDate(cdrSync)
    var startIntervalDate: DateTime = null
    var cdrCount = 0
    var lastUpdate = new DateTime(latestSyncDate, DateTimeZone.UTC)

    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = lastUpdate.getMillis() + updateInterval - DateTime.now(DateTimeZone.UTC).getMillis()
      if (sleepTime >= 0){
        Thread.sleep(sleepTime)
      }

      logger.info(s"Syncing CDR's since $lastUpdate")

      // Reset loop variables
      var msgCount = 0
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)
      val startTime = System.nanoTime()
      var newestTsMs : Long = 0
      val cdrFetch = Try {
        cdrRdd.select("created_at", "event_details", "service", "used_service_units", "event_charges")
          .where("created_at > ?", timeLimit.toString()).withAscOrder
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1
          cdrCount += 1

          val service = row.getString("service")
          val timeStamp = row.getDateTime("created_at")
          if (timeStamp.getMillis > newestTsMs){
              newestTsMs = timeStamp.getMillis
          }
          val eventDetails = row.getUDTValue("event_details")
          val isRoaming = eventDetails.getBoolean("is_roaming")

          // Select a_party country
          val aPartyLocation = eventDetails.getUDTValue("a_party_location")
          val eventCharges = row.getUDTValue("event_charges")
          val product =  eventCharges.getUDTValue("product") // investigate
          val productName = product.getString("name")
          val aPartyDestination = aPartyLocation.getString("destination")
          val aPartyCountryCode = aPartyDestination.substring(0, 3)
          val aPartyCountryISO = countries(aPartyCountryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)

          /*
          // Select b_party country
          if(service == "voice"){
            val bPartyLocation = eventDetails.getUDTValue("b_party_location")
            val bPartyDestination = bPartyLocation.getString("destination")
            val bPartyCountryCode = bPartyDestination.substring(0, 3)
            val bPartyCountryISO = countries(bPartyCountryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)
          }*/
          // Select used_service_units
          val usedServiceUnits = row.getUDTValue("used_service_units")
          val amount = usedServiceUnits.getInt("amount")

          // Add datapoint to dispatcher
          // TODO fix count for specific items
          startIntervalDate match {
            case null => startIntervalDate = timeStamp
            case _ => {
              val seconds = Seconds.secondsBetween(startIntervalDate, timeStamp).getSeconds
              if (seconds >= 10) {
                val value = cdrCount / 10
                if (isRoaming && service.equals("voice")) {
                  dispatcher.append(s"qvantel.call.$service.destination.$aPartyCountryISO", value.toString, timeStamp)
                }
                dispatcher.append(s"qvantel.product.$productName", value.toString, timeStamp)
                cdrCount = 0
                startIntervalDate = timeStamp
              }
            }
          }
          lastUpdate = timeStamp
        })
      }

      cdrFetch match {
        case Success(_) if msgCount > 0  => {
          commitBatch(dispatcher, msgCount)
          updateLatestSync("cdrsync", new DateTime(newestTsMs))
          val endTime = System.nanoTime()
          val throughput = measureDataSendPerSecond(startTime, endTime, msgCount)
          dispatcher.append(s"qvantel.dbconnector.throughput.call", throughput.toString, DateTime.now(DateTimeZone.UTC))
        }
        case Success(_) if msgCount == 0  => logger.info("Was not able to fetch any new CDR row from Cassandra")
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

    logger.info(result + " cdrs/second")
    result
  }
}
