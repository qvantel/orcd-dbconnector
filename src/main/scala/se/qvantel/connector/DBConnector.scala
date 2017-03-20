package se.qvantel.connector

import com.datastax.spark.connector._
import com.datastax.spark.connector.{CassandraRow, SomeColumns}
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.joda.time.{DateTime, DateTimeZone}
import property.{CountryCodes, Logger, Processing}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Random, Success, Try}

case class SyncModel(id: Int, ts: DateTime)

object DBConnector extends SparkConnection
  with CountryCodes with Logger with Processing {

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
    logger.info("Starting processing of CALLS and PRODUCTS")
    val f1 = Future(callProcessing(dispatcher))
    val f2 = Future(productProcessing(dispatcher))
   // Waiting for just one Future as there is no point running if either product or call fails
    Await.result(f1, Duration.Inf)
  }

  def callProcessing(dispatcher: DatapointDispatcher): Unit = {
    val rdd = context.cassandraTable("qvantel", "call")
    val callSync = context.cassandraTable("qvantel", "callsync")
    val latestSyncDate = getLatestSyncDate(callSync)

    var lastUpdate = new DateTime(latestSyncDate)

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
        rdd.select("created_at", "event_details", "service", "used_service_units")
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
          val isRoaming = eventDetails.getBoolean("is_roaming")

          // Select used_service_units
          val usedServiceUnits = row.getUDTValue("used_service_units")
          val amount = usedServiceUnits.getInt("amount")

          // Add datapoint to dispatcher
          if(isRoaming){
            dispatcher.append(s"qvantel.call.$service.destination.$countryISO", amount.toString, timeStamp)
          }
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
    val rdd = context.cassandraTable("qvantel", "product")
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

      val productFetch = Try {
        rdd.select("created_at", "event_details", "service", "used_service_units", "event_charges")
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

  def commitBatch(dispatcher: DatapointDispatcher, msgCount: Int): Unit = {
    dispatcher.append(s"qvantel.dbconnector.throughput", msgCount.toString, new DateTime(DateTimeZone.UTC))
    dispatcher.dispatch()
    logger.info(s"Sent a total of $msgCount datapoints to carbon this iteration")
  }

  def getLatestSyncDate(rdd: CassandraTableScanRDD[CassandraRow]): Long = {
    if (rdd.count() > 0) {
      rdd.first().get[Long]("ts")
    } else {
      0 // sync time will be set to POSIX time
    }
  }

  def updateLatestSync(tableName: String): Unit = {
    // Insert current time stamp for syncing here.
    // Insert timestamp always on id=1 to only have one record of a timestamp.
    val date = DateTime.now()
    val collection = context.parallelize(Seq(SyncModel(1,date)))
    collection.saveToCassandra("qvantel", tableName, SomeColumns("id","ts"))
  }

}
