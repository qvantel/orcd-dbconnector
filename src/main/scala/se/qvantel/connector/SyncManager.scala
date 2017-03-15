package se.qvantel.connector
import com.datastax.spark.connector.{CassandraRow, SomeColumns}
import org.joda.time.DateTime
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector._

trait SyncManager extends SparkConnection {


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
