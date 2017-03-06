import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}

trait SparkConnection {
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

  // create new table for the time sync if not exists
  session.execute("CREATE TABLE IF NOT EXISTS qvantel.latestsync(id INT PRIMARY KEY, ts timestamp)")
}
