package se.qvantel.connector
import com.datastax.spark.connector._
import org.joda.time.{DateTime, DateTimeZone}
import se.qvantel.connector.DBConnector._
import se.qvantel.connector.property.SparkConfig

import scala.util.{Failure, Success, Try}

class ProcessingManager extends SparkConfig {

  def cdrProcessing(dispatcher: DatapointDispatcher): Unit = {

    val cdrRdd = context.cassandraTable(keySpace, cdrTable)
    val cdrSync = context.cassandraTable(keySpace, cdrSyncTable)
    val latestSyncDate = getLatestSyncDate(cdrSync)
    var lastUpdate = 0L

    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = ((lastUpdate*1000) + updateInterval) - System.currentTimeMillis()
      if (sleepTime >= 0){
        Thread.sleep(sleepTime)
      }

      val lastUpdateDate = new DateTime(lastUpdate*1000L)
      logger.info(s"Syncing CDR's since $lastUpdateDate")

      // Reset loop variables
      var msgCount = 0
      val lastUpdateInMicro = lastUpdate*1000000
      val startTime = System.nanoTime()
      var newestTsMs = 0L
      val cdrFetch = Try {
        cdrRdd.select("created_at", "event_details", "service", "used_service_units", "event_charges")
          .where("created_at > ?", lastUpdateInMicro).where("clustering_key=0").clusteringOrder(rdd.ClusteringOrder.Ascending)
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          val service = row.getString("service")
          val unixMicroTimeStamp = row.getLong("created_at")
          val unixMiliTimeStamp = unixMicroTimeStamp/1000
          val timeStamp = unixMicroTimeStamp/1000000
          if (unixMiliTimeStamp > newestTsMs){
              newestTsMs = unixMiliTimeStamp
          }
          val eventDetails = row.getUDTValue("event_details")
          val isRoaming = eventDetails.getBoolean("is_roaming")

          // Select a_party country
          val aPartyLocation = eventDetails.getUDTValue("a_party_location")
          val eventCharges = row.getUDTValue("event_charges")
          val product =  eventCharges.getUDTValue("product")
          val productName = product.getString("name").replaceAll(" ", "")
          val aPartyDestination = aPartyLocation.getString("destination")
          val aPartyCountryCode = aPartyDestination.substring(0, 3)
          val aPartyCountryISO = countries(aPartyCountryCode) // Map MCC to country ISO code (such as "se", "dk" etc.)

          // Select used_service_units
          val usedServiceUnits = row.getUDTValue("used_service_units")
          val amount = usedServiceUnits.getInt("amount")

          // Add datapoint to dispatcher
          dispatcher.append(s"qvantel.product.$productName", timeStamp)
          if (isRoaming) {
            dispatcher.append(s"qvantel.call.$service.destination.$aPartyCountryISO", timeStamp)
          }

          if (lastUpdate < timeStamp) {
            lastUpdate = timeStamp
          }
        })
      }

      cdrFetch match {
        case Success(_) if msgCount > 0  => {
          //commitBatch(dispatcher, msgCount, lastUpdate.toString)
          updateLatestSync(new DateTime(newestTsMs, DateTimeZone.UTC))
          val endTime = System.nanoTime()
          val throughput = measureDataSendPerSecond(startTime, endTime, msgCount)
          //dispatcher.append(s"qvantel.dbconnector.throughput.call", throughput.toString, DateTime.now(DateTimeZone.UTC))
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
