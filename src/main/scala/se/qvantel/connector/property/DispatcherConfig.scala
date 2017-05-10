package se.qvantel.connector.property

trait DispatcherConfig extends Config {
  val timeout = config.getInt("dispatcher.timeout")
  val timeStampInterval = config.getInt("dispatcher.timeStampIntervalSeconds")
  val graphiteTimeoutReconnectionMs = config.getInt("dispatcher.graphiteTimeoutReconnectionMs")
}
