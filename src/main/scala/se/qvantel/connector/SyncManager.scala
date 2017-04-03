package se.qvantel.connector
import com.datastax.spark.connector.{CassandraRow, SomeColumns}
import org.joda.time.DateTime
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector._
import se.qvantel.connector.DBConnector.logger

case class SyncModel(id: Int, ts: DateTime)

trait SyncManager extends SparkConnection {

  private var benchmark = false

  def syncLoop(dispatcher: DatapointDispatcher, benchmark: Boolean): Unit = {
    this.benchmark = benchmark
    logger.info("Starting processing of CDR")
    val pm = new ProcessingManager()
    pm.cdrProcessing(dispatcher)
  }

  def getLatestSyncDate(rdd: CassandraTableScanRDD[CassandraRow]): Long = {
    // refactor to remove 0 and 1 to make it more readable
    rdd.count() match {
      case 0 => 0
      case 1 => {
        benchmark match {
          case false => rdd.first().get[Long]("ts")
          case true => 0
        }
      }
    }
  }

  def updateLatestSync(tableName: String, ts: DateTime): Unit = {
    // Insert current time stamp for syncing here.
    // Insert timestamp always on id=1 to only have one record of a timestamp.
    val collection = context.parallelize(Seq(SyncModel(1,ts)))
    collection.saveToCassandra("qvantel", tableName, SomeColumns("id","ts"))
  }
}
