package se.qvantel.connector.property

import se.qvantel.connector.model.Country
import org.json4s.{DefaultFormats, _}
import org.json4s.native.JsonMethods._
import scala.collection.mutable.HashMap
import java.io.{InputStream}

trait CountryCodes extends Config {
  val countriesFile = config.getString("gen.countries.file")
  val countries = HashMap.empty[String, String]

  def getCountriesByMcc(): Unit = {
    // Open a source file
    val source : InputStream = getClass.getResourceAsStream(countriesFile)
    val lines = scala.io.Source.fromInputStream( source ).mkString

    // For json4s, specify parse format
    implicit val format = DefaultFormats

    // Parse the contents, extract to a list of countries
    val countriesList = parse(lines).extract[List[Country]]

    // Close source file
    source.close()

    // Take the Country.mcc and make it's own list with only the distinct values
    countriesList.map(c => c.mcc).distinct

    // Map mcc code to iso
    countriesList.foreach(c => {
      countries.put(c.mcc, c.iso)
    })
  }
}
