package property

import com.typesafe.scalalogging.Logger

trait Logger {
  // Set up logging
  val logger = Logger("DBConnector")
}
