package se.qvantel.connector.property

import org.scalatest.FunSuite

class ProcessingTest extends FunSuite with ProcessingConfig {

  test("That a valid processing config exists") {
    assert(fetchBatchSize > 0 && fetchBatchSize <= 20000)
    assert(updateInterval > 0 && updateInterval <= 10000)
  }
}
