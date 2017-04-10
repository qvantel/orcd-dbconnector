package se.qvantel.connector.property

import org.scalatest.FunSuite

class SparkConfigTest extends FunSuite with SparkConfig {
  test("Check that spark configuration is present") {
    // Note: Sparkconfig is extended in the test case class

    val port = cassandraPort.toInt
    assert(port > 0 && port < 65535)
    assert(hostName.length() > 0)
    assert(keySpace.length() > 0)
    assert(!userName.isEmpty)
    assert(!password.isEmpty)
    assert(cdrTable.length() > 0)
    assert(cdrSyncTable.length() > 0)
  }

}
