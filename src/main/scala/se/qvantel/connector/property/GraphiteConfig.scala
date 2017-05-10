package se.qvantel.connector.property

trait GraphiteConfig extends Config {
  val graphiteHost = config.getString("graphite.host")

  val graphitePort = config.getInt("graphite.port")
  val connectionFailureMsg = "Failed to setup socket for Carbon at host: " + graphiteHost + ":" + graphitePort
}
