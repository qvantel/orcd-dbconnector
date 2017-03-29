package se.qvantel.connector

import java.io._
import java.net._

import org.joda.time.DateTime
import property.Logger

import scala.collection.mutable
import scala.util.Try

class DatapointDispatcher(ip: String, port: Int) extends Logger {

  val socket = new Socket()
  val graphiteAddress = new InetSocketAddress(ip, port)

  val batchSize = 250
  var messagesSent = 0
  var messageQueue = 0

  var countedRecords =  mutable.HashMap.empty[String, Int]

  def connect(): Try[Unit] = {
    val timeout = 5000
    Try(socket.connect(graphiteAddress, timeout))
  }

  def append(destination: String, value: String, timestamp: DateTime): Unit = {
    messageQueue = messageQueue + 1

    if (!countedRecords.contains(destination)) {
      countedRecords.put(destination, 0)
    }
    countedRecords.put(destination, countedRecords(destination) + 1)

    val timestampstr = (timestamp.getMillis() / 1000L).toString()
    if (messageQueue >= batchSize) {
      dispatch(timestampstr)
    }
  }

  def dispatch(ts: String): Unit = {
    // Socket output stream
    val out = new PrintStream(socket.getOutputStream)

    // Log and count messages sent
    messagesSent += messageQueue
    messageQueue = 0
    //logger.info("Sending " + messageQueue.length + s" datapoints to carbon, have now sent a total of $messagesSent")

    // Prepare payload
    /*var payload = ""
    messageQueue.dequeueAll(datapoint => {
      payload += datapoint + "\n"
      true
    })*/

    // Send payload
    var payload = ""
    countedRecords.foreach(p => payload +=  s"${p._1} ${p._2} $ts \n") //append payload
    logger.info(payload)
    out.print(payload)
    countedRecords.clear()
  }

  def close(): Unit = socket.close()
}
