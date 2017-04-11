package se.qvantel.connector

import java.nio.charset.StandardCharsets

import org.joda.time.DateTime
import org.scalatest.FunSuite

case class Metric(str: String, ts : Long)

class DatapointDispatcherTest extends FunSuite {

  // Leaving this commented in case we might need it
  /*
  test("Test dispatching metrics to a mock-up stream") {
    val dispatcher = new DatapointDispatcher()
    dispatcher.disableAutoSend()

    val destinations = List("qvantel.call.voice.destination.se", "qvantel.product.championsleague")
    val timeStamp = 1491295982

    val foo = List(
      Metric(destinations.head, 0),
      Metric(destinations.head, 0),
      Metric(destinations(1),   0),
      Metric(destinations(1),   0),
      Metric(destinations(1),   0),
      Metric(destinations.head, 0),
      Metric(destinations.head, 0)
    )

    foo.foreach(metric => dispatcher.append(metric.str, metric.ts))
    dispatcher.dispatch(timeStamp)

    val str = dispatcher.baos match {
      case Some(b) => {
        new String(b.toByteArray, StandardCharsets.UTF_8)
          .split("\n")
          .map(str => str.trim())
      }
      case None => fail("Baos is not set")
    }

    assert(str(0).equals(s"${destinations(1)} 3 ${timeStamp}"))
    assert(str(1).equals(s"${destinations.head} 4 ${timeStamp}"))
  }

  test("Test autosend works") {
    val dispatcher = new DatapointDispatcher()
    val destination = "qvantel.product.championsleague"
    val metric = Metric(destination, 0)

    val ts = DateTime.now.getMillis
    dispatcher.append(metric.str, ts)
    val ts2 = ts + 10000
    dispatcher.append(metric.str, ts2)

    val str = dispatcher.baos match {
      case Some(b) => {
        new String(b.toByteArray, StandardCharsets.UTF_8)
          .split("\n")
          .map(str => str.trim())
      }
      case None => fail("baos is not set")
    }

    assert(str(0).equals(s"${destination} 2 ${ts}"))
  }
  */
}
