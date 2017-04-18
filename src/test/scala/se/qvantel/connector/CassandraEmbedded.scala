package se.qvantel.connector

import java.net.InetAddress

import com.datastax.driver.core.{Cluster, Session}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, YamlTransformations}
import com.datastax.spark.connector.util.Logging
import org.apache.commons.collections.map.TypedMap
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.collection.mutable


trait CassandraEmbedded extends FunSuite with Eventually
  with EmbeddedCassandra
  with Logging
  with BeforeAndAfterAll {

  val sd = EmbeddedCassandra
  val testKeyspace = "testKeySpace"
  val testTable = "testTable"


  EmbeddedCassandraServerHelper.startEmbeddedCassandra()


  def getSession(): Unit ={
    //println("---sessToString----->" + sess.toString())
  }

  useCassandraConfig(Seq(YamlTransformations.ClientEncryption))

//  val host = Set(EmbeddedCassandra.getHost(0))
//  val port = EmbeddedCassandra.getPort(0)
//  def getPort(): Unit ={
//    val dd = InetAddress.getLocalHost
//    println("----poort-------"+conn.port)
//    println("---localHos--->" + dd.toString())
//  }

//  val dd = InetAddress.getLocalHost
//  val ports = "/127.0.0.1"
//  var conn: CassandraConnector = CassandraConnector(Set(EmbeddedCassandra.getHost(0))) //Set(EmbeddedCassandra.getHost(0))
//    conn.withSessionDo { session =>
//      session.execute( s"""DROP KEYSPACE IF EXISTS $testKeyspace""")
//      session.execute( s"""CREATE KEYSPACE $testKeyspace
//                          |WITH replication = {
//                          |'class': 'SimpleStrategy',
//                          |'replication_factor':1}
//                          |""".stripMargin)
//      session.execute(s"""CREATE TABLE $testKeyspace.$testTable(
//                         | partitionKey text,
//                         | clusterKey text,
//                         | someValue int,
//                         | PRIMARY KEY (partitionKey, clusterKey))
//                         | WITH
//                         | compaction = { 'class' : 'SizeTieredCompactionStrategy'} AND
//                         | compression = { 'sstable_compression' : 'SnappyCompressor' }
//                         | """.stripMargin)
//    }
}
