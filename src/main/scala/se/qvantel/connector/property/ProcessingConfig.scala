package se.qvantel.connector.property

trait ProcessingConfig extends Config {
  val batchSize = config.getInt("processing.batchSize")
  val updateInterval = config.getInt("processing.updateInterval")
  val fetchBatchSize = config.getInt("processing.fetchBatchSize")
  var limit = config.getInt("processing.fetchBatchSize")
}
