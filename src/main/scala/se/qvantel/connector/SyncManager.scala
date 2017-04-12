package se.qvantel.connector
import com.datastax.spark.connector.{CassandraRow, SomeColumns}
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector._
import com.typesafe.scalalogging.LazyLogging

case class SyncModel(id: Int, ts: Long)

trait SyncManager extends SparkConnection with LazyLogging {

  private var benchmark = false
  type CassandraRDD = CassandraTableScanRDD[CassandraRow]

  def syncLoop(benchmark: Boolean): Unit = {
    this.benchmark = benchmark
    logger.info("Starting processing of CDR")
    new ProcessingManager().cdrProcessing()
  }

  /**
    * @return 0 -> Posix time , X -> latest date fetched from cassandra
    */
  def getLatestSync(rdd: CassandraRDD): Long = {
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

  def updateLatestSync(tsUs: Long): Unit = {
    // Insert current time stamp for syncing here.
    // Insert timestamp always on id=0 to only have one record of a timestamp.
    val collection = context.parallelize(Seq(SyncModel(0,tsUs)))
    collection.saveToCassandra(keySpace, cdrSyncTable, SomeColumns("id", "ts"))
  }
}
