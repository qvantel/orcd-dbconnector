package se.qvantel.connector
import com.datastax.spark.connector._
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime
import se.qvantel.connector.DBConnector._
import se.qvantel.connector.property.SparkConfig

import scala.util.{Failure, Success, Try}

class ProcessingManager extends SparkConfig with LazyLogging {

  def cdrProcessing(dispatcher: DatapointDispatcher): Unit = {
    val cdrRdd = context.cassandraTable(keySpace, cdrTable)
    val cdrSync = context.cassandraTable(keySpace, cdrSyncTable)
    var lastUpdateNs = getLatestSync(cdrSync)

    while (true) {
      // Sleep if lastUpdate was not back-in-time
      val sleepTime = ((lastUpdateNs/1000000L) + updateInterval) - System.currentTimeMillis()
      if (sleepTime >= 0){
        Thread.sleep(sleepTime)
      }

      val lastUpdateDate = new DateTime(lastUpdateNs/1000000L)
      logger.info(s"Syncing CDR's since $lastUpdateDate")

      // Reset loop variables
      var msgCount = 0
      val startTime = System.nanoTime()
      var newestTsNs = 0L
      val cdrFetch = Try {
        cdrRdd.select("created_at", "event_details", "service", "used_service_units", "event_charges")
          .where("created_at > ?", lastUpdateNs).where("clustering_key=0").clusteringOrder(rdd.ClusteringOrder.Ascending)
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          val service = row.getString("service")
          val tsNs = row.getLong("created_at")
          val ts = tsNs/1000000000L
          if (tsNs > newestTsNs){
              newestTsNs = tsNs
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

          // Add datapoint to dispatcher
          dispatcher.append(s"qvantel.product.$service.$productName", ts)
          if (isRoaming) {
            dispatcher.append(s"qvantel.call.$service.destination.$aPartyCountryISO", ts)
          }

          if (lastUpdateNs < tsNs) {
            lastUpdateNs = tsNs
          }
        })
      }

      cdrFetch match {
        case Success(_) if msgCount > 0 =>  {
          updateLatestSync(newestTsNs)
          val endTime = System.nanoTime()
          val throughput = measureDataSendPerSecond(startTime, endTime, msgCount)
        }
        case Success(_) if msgCount == 0 => logger.info("Was not able to fetch any new CDR row from Cassandra")
        case Failure(e) => e.printStackTrace()
      }
    }
  }

  private def measureDataSendPerSecond(startTime: Long, endTime: Long, msgCounter: Int): Double = {
    val nanosec = 1000000000.0
    val timedelta = (endTime - startTime) / nanosec
    var result = 0.0
    if (timedelta > 0) { // Handle possible division by zero
      result = msgCounter / timedelta
    }

    logger.info(result + " cdrs/second")
    result
  }
}
