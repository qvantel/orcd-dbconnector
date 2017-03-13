package se.qvantel.connector.property

import java.io.InputStream
import org.scalatest.FunSuite

class CountryCodesTest extends FunSuite with Config with CountryCodes {

  test("Check that there is a mcc-resources file") {
    try {
      val res = config.getString("gen.countries.file")

      val stream : InputStream = getClass.getResourceAsStream(res)
      val lines = scala.io.Source.fromInputStream(stream).mkString

      assert(lines.nonEmpty)
      stream.close()
    }
    catch {
      case ex: Exception => {
        fail(ex.getMessage())
      }
    }
  }

  test("Mapping of mcc to country") {
    // Loads MCC and countries ISO code into a HashMap, variable in CountryCodes
    getCountriesByMcc()
    // Check we actually map something
    assert(!countries.isEmpty)
    // Check if code 240 is mapped to se (Sweden)
    val swedenISO = countries("240")
    assert(swedenISO == "se")
    // Check if code 432 is mapped to ir (Iran)
    val iranISO = countries("432")
    assert(iranISO == "ir")
  }

}
