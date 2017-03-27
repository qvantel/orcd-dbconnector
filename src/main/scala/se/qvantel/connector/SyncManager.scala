package se.qvantel.connector
import com.datastax.spark.connector.{CassandraRow, SomeColumns}
import org.joda.time.DateTime
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector._
import se.qvantel.connector.DBConnector.logger
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent._
import ExecutionContext.Implicits.global

case class SyncModel(id: Int, ts: DateTime)

trait SyncManager extends SparkConnection {

  private var syncSwitcher = 0

  def syncLoop(dispatcher: DatapointDispatcher, syncNr: Int): Unit = {

    syncSwitcher = syncNr
    logger.info("Starting processing of CALLS and PRODUCTS")
    val processmanage = new ProcessingManager()
    val f1 = Future(processmanage.callProcessing(dispatcher))
    val f2 = Future(processmanage.productProcessing(dispatcher))


    // Waiting for just one Future as there is no point running if either product or call fails
    Await.result(f1, Duration.Inf)
  }


  def getLatestSyncDate(rdd: CassandraTableScanRDD[CassandraRow]): Long = {
    if (rdd.count() > 0 && syncSwitcher == 1) {
      rdd.first().get[Long]("ts")
    }
    else if(rdd.count() > 0 && syncSwitcher == 0) {
      0 // sync time will be set to POSIX time
    }
    else {
      0
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
