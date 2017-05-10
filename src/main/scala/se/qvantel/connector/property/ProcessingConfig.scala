package se.qvantel.connector.property

trait ProcessingConfig extends Config {
  val updateInterval = config.getInt("processing.updateInterval")
  val fetchBatchSize = config.getInt("processing.fetchBatchSize")
}
