package se.qvantel.connector
import org.joda.time.{DateTime, DateTimeZone}
import se.qvantel.connector.DBConnector._
import se.qvantel.connector.{DatapointDispatcher, SparkConnection, property}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import scala.util.{Failure, Random, Success, Try}

class ProcessingManager {

  val callRdd = context.cassandraTable("qvantel", "call")
  val productRdd = context.cassandraTable("qvantel", "product")
  var checkBool = true

  def callProcessing(dispatcher: DatapointDispatcher): Unit = {

    val callSync = context.cassandraTable("qvantel", "callsync")
    var latestSyncDate = getLatestSyncDate(callSync)
    if(checkBool == false) {
        latestSyncDate = 0
    }



    var lastUpdate = new DateTime (latestSyncDate)

    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = lastUpdate.getMillis() + updateInterval - DateTime.now(DateTimeZone.UTC).getMillis()
      if (sleepTime >= 0) {
        Thread.sleep(sleepTime)
      }

      logger.info(s"Syncing CALLS since $lastUpdate")

      // Reset loop variables
      var msgCount = 0
      val timeLimit = lastUpdate
      lastUpdate = DateTime.now(DateTimeZone.UTC)

      val callFetch = Try {
        callRdd.select("created_at", "event_details", "service", "used_service_units")
          .where("created_at > ?", timeLimit.toString()).withAscOrder
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          val service = row.getString("service")
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
      }

      callFetch match {
        case Success(_) if msgCount > 0  => {
          commitBatch(dispatcher, msgCount)
          updateLatestSync("callsync")
        }
        case Success(_) if msgCount == 0  => logger.info("Was not able to fetch any new CALL row from Cassandra")
        case Failure(e) => e.printStackTrace()
      }

    }
  }

  def productProcessing(dispatcher: DatapointDispatcher): Unit = {
    val productSync = context.cassandraTable("qvantel", "productsync")
    var latestSyncDate = getLatestSyncDate(productSync)

    if(checkBool == false)
    {
      latestSyncDate = 0
    }

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
        }
        case Success(_) if msgCount == 0 => logger.info("Was not able to fetch any new PRODUCT row from Cassandra")
        case Failure(e) => e.printStackTrace()
      }

    }
  }



  def getBooleanValue(checkNr: Int): Unit =
  {
    checkNr match
      {
      case 0 =>{checkBool = false}
      case 1 =>{checkBool = true}
    }
  }



}