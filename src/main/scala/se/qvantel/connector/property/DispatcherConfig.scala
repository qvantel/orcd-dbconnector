package se.qvantel.connector.property

trait DispatcherConfig extends Config {
  val timeout = config.getInt("dispatcher.timeout")
}
