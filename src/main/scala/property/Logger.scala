package property

import com.typesafe.scalalogging.{Logger => ScalaLogger}

trait Logger {
  val logger = ScalaLogger("DBConnector")
}
