package se.qvantel.connector.property

trait DispatcherConfig extends Config {
  val timeStampInterval = config.getInt("dispatcher.timeStampIntervalSeconds")
  val graphiteTimeoutReconnectionMs = config.getInt("dispatcher.graphiteTimeoutReconnectionMs")
}
