package se.qvantel.connector.property

trait Dispatcher extends Config{
  val timeout = config.getInt("dispatcher.timeout")
  val timeStampInterval = config.getInt("dispatcher.timeStampIntervalSeconds")
}
