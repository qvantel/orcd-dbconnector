package se.qvantel.connector

import java.io._
import java.net._

import property.Logger
import sys.process._
import scala.collection.mutable
import scala.util.Try

class DatapointDispatcher(ip: String, port: Int) extends Logger {

  val socket = new Socket()
  val graphiteAddress = new InetSocketAddress(ip, port)
  var startIntervalDate = 0L
  var elementsInBatch = 0
  var messagesSent = 0
  type CdrCount = Int
  type Destination = String
  var countedRecords =  mutable.HashMap.empty[Destination, CdrCount]

  def connect(): Try[Unit] = {
    val timeout = 200
    Try(socket.connect(graphiteAddress, timeout))
  }

  def append(destination: String, timeStamp: Long): Unit = {
    val timeStampInterval = 10

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
    val out: PrintStream = new PrintStream(socket.getOutputStream)

    // Log and count messages sent
    messagesSent += elementsInBatch
    elementsInBatch = 0

    // Send payload
    val payload = countedRecords.map(p => s"${p._1} ${p._2.toString} ${ts} ")
      .mkString("\n")

    out.print(payload)
    /*
    isConnected() match {
      case true => {
        // Socket output stream
        val out: PrintStream = new PrintStream(socket.getOutputStream)

        // Log and count messages sent
        messagesSent += elementsInBatch
        elementsInBatch = 0

        // Send payload
        val payload = countedRecords.map(p => s"${p._1} ${p._2.toString} ${ts} ")
          .mkString("\n")

        out.print(payload)
      }
      case false => {
        logger.error("Will attempt to reconnect, with a timeout")
        checkConnectionLoop()
        dispatch(ts)
      }
    }
    */
  }

  /*

  /** Attempts to reconnect to Carbon
    * Timeout for check is 5 seconds
    */
  def checkConnectionLoop(): Unit = {
    while (!isConnected()) {
      Thread.sleep(5000)
    }
  }


  /** Checks connection with Carbon
    *
    * Uses bash to check connection to ip:port
    * @return true on connected, otherwise false
    */
  def isConnected(): Boolean = {
    val address = s"</dev/tcp/${ip}/${port} 2>/dev/null"

    s"timeout -t2 bash -c ${address}; echo ${"$?"} | grep [0-9]" ! match {
      case 0 => true
      case 1 => false
    }
  }
  */


  def close(): Unit = socket.close()
}
