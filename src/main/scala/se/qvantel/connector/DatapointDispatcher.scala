package se.qvantel.connector

import java.io._
import java.net._

import org.joda.time.DateTime
import property.Logger

import scala.collection.mutable.Queue
import scala.util.Try

class DatapointDispatcher(ip: String, port: Int) extends Logger {

  val socket = new Socket()
  val graphiteAddress = new InetSocketAddress(ip, port)

  val batchSize = 250
  var messagesSent = 0
  var messageQueue = new Queue[String]

  def connect(): Try[Unit] = {
    val timeout = 5000
    Try(socket.connect(graphiteAddress, timeout))
  }

  def append(destination: String, value: String, timestamp: DateTime): Unit = {
    val timestampstr = (timestamp.getMillis() / 1000L).toString()
    messageQueue += s"$destination $value $timestampstr"
    if (messageQueue.size >= batchSize) {
      dispatch()
    }
  }

  def dispatch(): Unit = {
    // Socket output stream
    val out = new PrintStream(socket.getOutputStream)

    // Log and count messages sent
    messagesSent += messageQueue.length
    logger.info("Sending " + messageQueue.length + s" datapoints to carbon, have now sent a total of $messagesSent")

    // Prepare payload
    var payload = ""
    messageQueue.dequeueAll(datapoint => {
      payload += datapoint + "\n"
      true
    })
    // Send payload
    out.print(payload)
  }

  def close(): Unit = socket.close()
}
