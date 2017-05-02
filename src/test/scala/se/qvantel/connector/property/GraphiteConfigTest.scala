package se.qvantel.connector.property

import org.scalatest.FunSuite


class GraphiteConfigTest extends FunSuite with GraphiteConfig {

  test("Test graphpite config") {
    assert(graphitePort > 0 && graphitePort <= 65535)
    assert(!graphiteHost.isEmpty)
  }
}
