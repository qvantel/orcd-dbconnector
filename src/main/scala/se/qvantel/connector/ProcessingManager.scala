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
    var lastUpdateUs = getLatestSync(cdrSync)

    while (true) {
      // Sleep $updateInterval since lastUpdate
      val sleepTime = ((lastUpdateUs/1000) + updateInterval) - System.currentTimeMillis()
      if (sleepTime >= 0){
        Thread.sleep(sleepTime)
      }

      val lastUpdateDate = new DateTime(lastUpdateUs/1000L)
      logger.info(s"Syncing CDR's since $lastUpdateDate")

      // Reset loop variables
      var msgCount = 0
      var newestTsUs = 0L
      val cdrFetch = Try {
        cdrRdd.select("created_at", "event_details", "service", "used_service_units", "event_charges")
          .where("created_at > ?", lastUpdateUs).where("clustering_key=0").clusteringOrder(rdd.ClusteringOrder.Ascending)
          .limit(fetchBatchSize).collect().foreach(row => {

          msgCount += 1

          val service = row.getString("service")
          val tsUs = row.getLong("created_at")
          val ts = tsUs/1000000
          if (tsUs > newestTsUs){
              newestTsUs = tsUs
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
          dispatcher.append(s"qvantel.product.$productName", ts)
          if (isRoaming) {
            dispatcher.append(s"qvantel.call.$service.destination.$aPartyCountryISO", ts)
          }

          if (lastUpdateUs < tsUs) {
            lastUpdateUs = tsUs
          }
        })
      }

      cdrFetch match {
        case Success(_) if msgCount > 0 => updateLatestSync(newestTsUs)
        case Success(_) if msgCount == 0 => logger.info("Was not able to fetch any new CDR row from Cassandra")
        case Failure(e) => e.printStackTrace()
      }
    }
  }
}
