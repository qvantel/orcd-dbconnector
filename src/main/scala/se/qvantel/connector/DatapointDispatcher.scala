package se.qvantel.connector

import java.io._
import java.net._

import org.joda.time.{DateTime, DateTimeZone, Seconds}
import property.Logger

import scala.collection.mutable
import scala.util.Try

class DatapointDispatcher(ip: String, port: Int) extends Logger {

  val socket = new Socket()
  val graphiteAddress = new InetSocketAddress(ip, port)
  var startIntervalDate = new DateTime(null, DateTimeZone.UTC)
  val timeStampInterval = 10
  var elementsInBatch = 0
  var messagesSent = 0
  var countedRecords =  mutable.HashMap.empty[String, Int]

  def connect(): Try[Unit] = {
    val timeout = 5000
    Try(socket.connect(graphiteAddress, timeout))
  }

  def append(destination: String, timeStamp: DateTime): Unit = {

    if (!countedRecords.contains(destination)) {
      countedRecords.put(destination, 0)
    }
    countedRecords.put(destination, countedRecords(destination) + 1)
    elementsInBatch += 1
    startIntervalDate match {
      case null => startIntervalDate = timeStamp
      case _ => {
        val seconds = Seconds.secondsBetween(startIntervalDate, timeStamp).getSeconds
        if (seconds >= timeStampInterval) {
          dispatch(startIntervalDate.toString)
          countedRecords.clear()
          startIntervalDate = new DateTime(timeStamp, DateTimeZone.UTC)
        }
      }
    }
  }

  def dispatch(ts: String): Unit = {
    // Socket output stream
    val out = new PrintStream(socket.getOutputStream)

    // Log and count messages sent
    messagesSent += elementsInBatch
    elementsInBatch = 0
    //logger.info("Sending " + messageQueue.length + s" datapoints to carbon, have now sent a total of $messagesSent")

    // Prepare payload
    /*var payload = ""
    messageQueue.dequeueAll(datapoint => {
      payload += datapoint + "\n"
      true
    })*/

    // Send payload
    var payload = ""
    countedRecords.foreach(p => payload +=  s"${p._1} ${(p._2)} ${ts} \n") //append payload
    logger.info(payload)
    out.print(payload)
    //out.print("qvantel.product.CallPlanNormal 73 1491210661")
    countedRecords.clear()
  }

  def close(): Unit = socket.close()
}
