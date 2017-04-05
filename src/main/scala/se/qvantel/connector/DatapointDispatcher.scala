package se.qvantel.connector

import java.io._
import java.net._

import property.Logger
import sys.process._
import scala.collection.mutable
import scala.util.Try

class DatapointDispatcher extends Logger {

  var socket = None: Option[Socket]
  var graphiteAddress = None: Option[InetSocketAddress]
  var baos = None: Option[ByteArrayOutputStream]
  var startIntervalDate = 0L

  var autoSend : Boolean = true

  // Time (in seconds) to wait in between sending the records.
  val timeStampInterval = 10

  // Output metric of how many metrics were send during $timeStampInterval
  var elementsInBatch = 0

  // Output metric of how many times a netcat/query was sent to graphite.
  var messagesSent = 0

  type CdrCount = Int
  type Destination = String
  // A map, pointing a destination to an integer. Call append to increment the value.
  var countedRecords =  mutable.HashMap.empty[Destination, CdrCount]


  // Wrap the objects into their Option and attempt to connect to Carbon
  def init(ip: String, port : Int) : Try[Unit] = {
    socket = Some(new Socket())
    graphiteAddress = Some(new InetSocketAddress(ip, port))
    Try(connect())
  }

  def disableAutoSend(): Unit = {
    autoSend = false
  }

  // Socket is set as an Scala Option, as we want to be able
  // to choose between two streams. 1= real socket, 2= mock object
  def connect(): Unit = {
    val timeout = 5000
    socket match {
      case Some(sock) => {
        graphiteAddress match {
          case Some(addr) => sock.connect(addr, timeout)
          case None => logger.error("Graphite address not set")
        }
      }
      case None => logger.info("Socket has not been initialized")
    }
  }

  def isTimeToSendRecords(timestamp : Long): Boolean = {
    if ((timestamp - startIntervalDate) >= timeStampInterval) {
      true
    }
    false
  }

  def sendMetrics(timeStamp: Long) : Unit = {
   startIntervalDate match {
      case 0 => startIntervalDate = timeStamp
      case _ => {
        if (autoSend && isTimeToSendRecords(timeStamp)) {
          dispatch(startIntervalDate)
          countedRecords.clear()
          startIntervalDate = timeStamp
        }
      }
    }
  }

  // OutputStream for socket, ByteArrayOutputStream for unit testing
  def getStream: Either[OutputStream, ByteArrayOutputStream] = {
    socket match {
      case None => Right(new ByteArrayOutputStream())
      case Some(sock) => Left(sock.getOutputStream)
    }
  }

  def append(destination: String, timeStamp: Long): Unit = {

    if (!countedRecords.contains(destination)) {
      countedRecords.put(destination, 0)
    }
    countedRecords.put(destination, countedRecords(destination) + 1)

    // Increment output metric
    elementsInBatch += 1

    // See if need to send metrics
    sendMetrics(timeStamp)
  }

  def dispatch(ts: Long): Unit = {
    // Socket output stream

    val out = getStream match {
      case Left(outputStream) => new PrintStream(outputStream)
      case Right(byteArrayOutputStream) => {
        baos = Some(byteArrayOutputStream)
        new PrintStream(baos.get)
      }
    }

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


  def close(): Unit = {
    socket match {
      case Some(sock) => sock.close()
      case None => logger.error("Trying to close socket without it being open")
    }
  }
}
