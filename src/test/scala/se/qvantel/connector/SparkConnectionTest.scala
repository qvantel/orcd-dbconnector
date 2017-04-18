package se.qvantel.connector

import com.datastax.spark.connector.embedded.EmbeddedCassandra
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions
import com.datastax.spark.connector.util.Logging
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkConnectionTest extends CassandraEmbedded {

  var sc: SparkContext = null


  val embCasPort = EmbeddedCassandra.getPort(0)
  val embCasHost = EmbeddedCassandra.getHost(0)
  println("----embCasPort----->" + embCasPort)
  println("----embCasHost----->" + embCasHost)
    val name = "sparkTtest"

//    val conf = new SparkConf()
//      .setAppName(name)
//      .setMaster("local")
//      .set("spark.default.parallelism", "1")
//
//    sc = new SparkContext(conf)
//    sc.stop()





  test("spark Configuration testing"){
//    val appname = "sparkConfTest"
//    // create spark config
//    val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
//    val sparkContx = new SparkContext(sparkConf)
//
//
//    sparkContx.stop()
  }

  test("spark point to cassandra"){
//    val appname = "sparkConfTest"
//
//    getSession()
//    // create spark config
//    val sparkConf = new SparkConf().setAppName(appname).setMaster("local")
//    sparkConf.set("spark.cassandra.connection.host", "1")
//    val sparkContx = new SparkContext(sparkConf)
//
//    sparkContx.stop()
  }

  override def clearCache(): Unit = ???

}
