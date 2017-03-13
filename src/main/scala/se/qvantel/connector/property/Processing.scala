package se.qvantel.connector.property


trait Processing extends Config {
  val batchSize = config.getInt("processing.batchSize")
  val updateInterval = config.getInt("processing.updateInterval")
  val fetchBatchSize = config.getInt("processing.fetchBatchSize")
}
