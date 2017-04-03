package se.qvantel.connector

import java.io._
import java.net._
import property.Logger
import scala.collection.mutable
import scala.util.Try

class DatapointDispatcher(ip: String, port: Int) extends Logger {

  val socket = new Socket()
  val graphiteAddress = new InetSocketAddress(ip, port)
  var startIntervalDate = 0L
  val timeStampInterval = 10
  var elementsInBatch = 0
  var messagesSent = 0
  var countedRecords =  mutable.HashMap.empty[String, Int]

  def connect(): Try[Unit] = {
    val timeout = 5000
    Try(socket.connect(graphiteAddress, timeout))
  }

  def append(destination: String, timeStamp: Long): Unit = {

    if (!countedRecords.contains(destination)) {
      countedRecords.put(destination, 0)
    }
    countedRecords.put(destination, countedRecords(destination) + 1)
    elementsInBatch += 1
    startIntervalDate match {
      case 0 => startIntervalDate = timeStamp
      case _ => {
        val seconds = timeStamp - startIntervalDate
        if (seconds >= timeStampInterval) {
          dispatch(startIntervalDate)
          countedRecords.clear()
          startIntervalDate = timeStamp
        }
      }
    }
  }

  def dispatch(ts: Long): Unit = {
    // Socket output stream
    val out = new PrintStream(socket.getOutputStream)

    // Log and count messages sent
    messagesSent += elementsInBatch
    elementsInBatch = 0

    // Send payload
    var payload = ""
    countedRecords.foreach(p => payload +=  s"${p._1} ${(p._2.toString)} ${ts} \n") //append payload
    logger.info(payload)
    out.print(payload)
  }

  def close(): Unit = socket.close()
}
