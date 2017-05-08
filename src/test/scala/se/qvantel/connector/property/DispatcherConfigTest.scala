package se.qvantel.connector.property

import org.scalatest.FunSuite

class DispatcherConfigTest extends FunSuite with DispatcherConfig {

  test("Test dispatcher config") {
    assert(timeout > 0)
    assert(graphiteTimeoutReconnectionMs > 0)
    assert(timeStampInterval > 0)
  }
}