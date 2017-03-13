package se.qvantel.connector.property

import com.typesafe.config.ConfigFactory

trait Config {
  val config = ConfigFactory.load()
}
