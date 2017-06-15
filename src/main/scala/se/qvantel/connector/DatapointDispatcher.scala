package se.qvantel.connector

import java.io._
import java.net._
import com.typesafe.scalalogging.LazyLogging
import property.{DispatcherConfig, GraphiteConfig}
import scala.collection.mutable
import scala.util.Try

class DatapointDispatcher extends LazyLogging with DispatcherConfig with GraphiteConfig {
  var startIntervalDate = 0L

  // Used for unit tests, to disable side effects
  // If true, then normal sockets are used
  var autoSend: Boolean = true
  var baos = None: Option[ByteArrayOutputStream]

  type CdrCount = Int
  type Destination = String
  // A map, pointing a destination to an integer. Call append to increment the value.

  var countedRecords = mutable.HashMap.empty[Destination, CdrCount]

  def disableAutoSend(): Unit = autoSend = false

  def connect(): Option[Socket] = Try {
    val graphiteAddress = new InetSocketAddress(graphiteHost, graphitePort)
    val sock = new Socket()
    sock.connect(graphiteAddress)
    sock
  }.toOption

  // If the saved timestamp exceeds $timeStampInterval, it's time to send to the metric database.
  def isTimeToSendRecords(timestamp: Long): Boolean =
    (timestamp - startIntervalDate) >= timeStampInterval

  def sendMetrics(timeStamp: Long): Unit = {
    startIntervalDate match {
      case 0L => startIntervalDate = timeStamp
      case _ => {
        if (autoSend && isTimeToSendRecords(timeStamp)) {
          if (dispatch(startIntervalDate).isFailure) {
            graphiteReconnectionLoop()
          }
          countedRecords.clear()
          startIntervalDate = timeStamp
        }
      }
    }
  }

  def append(destination: String, timeStamp: Long): Unit = {
    if (!countedRecords.contains(destination)) {
      countedRecords.put(destination, 0)
    }
    countedRecords.put(destination, countedRecords(destination) + 1)

    sendMetrics(timeStamp)
  }

  def dispatch(ts: Long): Try[Unit] = Try {
    val sockOpt = connect().headOption

    val stream = autoSend match {
      case true => new PrintStream(sockOpt.map(_.getOutputStream).head)
      case false => {
        baos = Some(new ByteArrayOutputStream())
        new PrintStream(baos.get)
      }
    }
    // Send payload
    val payload = countedRecords.map(p => s"${p._1} ${p._2.toString} $ts ")
      .mkString("\n")

    stream.print(payload)

    // Kill socket connection
    sockOpt.foreach(_.close())
  }

  private def graphiteReconnectionLoop(): Unit = {
    var connected = false

    while(!connected) {
      Thread.sleep(graphiteTimeoutReconnectionMs)
      connect() match {
        case Some(sock) => {
          connected = true
          sock.close()
        }
        case None => logger.info(connectionFailureMsg +
          ", will attempt again in " + graphiteTimeoutReconnectionMs/1000 + "seconds")
      }
    }
  }

}
