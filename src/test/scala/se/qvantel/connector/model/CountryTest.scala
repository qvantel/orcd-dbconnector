package se.qvantel.connector.model

import org.json4s.DefaultFormats
import org.scalatest.FunSuite
import org.json4s.native.JsonMethods._

class CountryTest extends FunSuite {

  test("Test parsing of JSON-string to model.Country case class") {
    val string = "[" +
      "{\"network\":\"A-Mobile\",\"country\":\"Abkhazia\",\"mcc\":\"289\",\"iso\":\"ge\"," +
      "\"country_code\":\"7\",\"mnc\":\"68\"}," +
      "{\"network\":\"A-Mobile\",\"country\":\"Abkhazia\"," +
      "\"mcc\":\"289\",\"iso\":\"ge\",\"country_code\":\"7\",\"mnc\":\"88\"}," +
      "{\"network\":\"Aquafon\"," +
      "\"country\":\"Abkhazia\",\"mcc\":\"289\",\"iso\":\"ge\",\"country_code\":\"7\",\"mnc\":\"67\"}" +
      "{\"network\":\"BCELL\"," +
      "\"country\":\"Belgium\",\"mcc\":\"100\",\"iso\":\"ge\",\"country_code\":\"8\",\"mnc\":\"67\"}" +
      "]"

    // Specify format method
    implicit val format = DefaultFormats

    // Parse the object
    val obj = parse(string).extract[List[Country]]

    // Cherry picked testing.
    assert(obj.head.network.equals("A-Mobile"))
    assert(obj.head.country.equals("Abkhazia"))
    assert(obj.head.mcc.equals("289"))

    assert(obj(1).network.equals("A-Mobile"))
    assert(obj(1).country.equals("Abkhazia"))
    assert(obj(1).mcc.equals("289"))

    assert(obj(2).network.equals("Aquafon"))
    assert(obj(2).country.equals("Abkhazia"))
    assert(obj(2).mcc.equals("289"))

    assert(obj(3).network.equals("BCELL"))
    assert(obj(3).country.equals("Belgium"))
    assert(obj(3).mcc.equals("100"))
  }
}
