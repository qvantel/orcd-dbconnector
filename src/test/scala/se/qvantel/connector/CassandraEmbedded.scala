package se.qvantel.connector

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, YamlTransformations}
import com.datastax.spark.connector.util.Logging
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite}



trait CassandraEmbedded extends FunSuite with Eventually
  with EmbeddedCassandra
  with Logging{

  val testKeyspace = "testKeySpace"
  val testTable = "testTable"
  var conn: CassandraConnector = _

    //useCassandraConfig(YamlTransformations("cassandra-default.yaml.template"))
    conn = CassandraConnector(Set(EmbeddedCassandra.getHost(0)))
    println("---host---->" + EmbeddedCassandra.getHost(0))
    conn.withSessionDo { session =>
      session.execute( s"""DROP KEYSPACE IF EXISTS $testKeyspace""")
      session.execute( s"""CREATE KEYSPACE $testKeyspace
                          |WITH replication = {
                          |'class': 'SimpleStrategy',
                          |'replication_factor':1}
                          |""".stripMargin)
      session.execute(s"""CREATE TABLE $testKeyspace.$testTable(
                         | partitionKey text,
                         | clusterKey text,
                         | someValue int,
                         | PRIMARY KEY (partitionKey, clusterKey))
                         | WITH
                         | compaction = { 'class' : 'SizeTieredCompactionStrategy'} AND
                         | compression = { 'sstable_compression' : 'SnappyCompressor' }
                         | """.stripMargin)
    }

}
