import org.apache.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.scalalogging.Logger
import java.net._
import java.io._
import scala.io._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import scala.pickling.Defaults._
import scala.pickling.Pickler

object DBConnector {

  def main(args: Array[String]): Unit =
  {
    // Set up logging
    val logger = Logger("CDRGenerator")



    // Configure spark->cassandra connection
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
    val context = new SparkContext("local[2]", "database", conf)

    // Setup cassandra connector
    val connector = CassandraConnector(conf)
    // Create cassandra session
    val session = connector.openSession()

    val rdd = context.cassandraTable("database", "cdr")

    val s = new Socket(InetAddress.getLocalHost(), 2003)
    lazy val in = new BufferedSource(s.getInputStream()).getLines()
    val out = new PrintStream(s.getOutputStream)

    //case class Datapoint(value: Int, ts: Long)
    //case class Entry(path: String, dp: Datapoint)
    //case class Request(list: List[Entry])

    var last_update = new DateTime(0)
    while (true) {
      while (last_update.getMillis()+2000 > DateTime.now(DateTimeZone.UTC).getMillis())
        Thread.sleep(100)

      val last_update_ms = last_update.getMillis() / 1000L
      println(s"Syncing since $last_update")
      //var data = Request
      rdd.select("ts", "key", "value").where("ts > ?", last_update.toString()).collect().foreach(row => {
        val value = row.getInt("value")
        val ts = (row.getDateTime("ts").getMillis() / 1000L)

        //val entry = Entry("database.cdr.value", Datapoint(value, ts))
        //data.apply(List(entry))

        val diff = ts-last_update_ms
        logger.debug(s"Sent $value at $ts, $diff")
        out.println(s"database.cdr.value $value $ts")
      })
      //out.println(data.pickle(pickling.binary.pickleFormat, Pickler.generate[Request.type]))
      last_update = DateTime.now(DateTimeZone.UTC)
    }
    s.close()

    // Close cassandra session
    session.close()
  }
}
