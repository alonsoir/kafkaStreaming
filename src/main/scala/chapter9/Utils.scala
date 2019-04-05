package chapter9

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Utils {

  def dfSchema(columnNames: List[String]): StructType =
    StructType(
      Seq(
        StructField(name = "country", dataType = StringType, nullable = false),
        StructField(name = "time", dataType = IntegerType, nullable = false),
        StructField(name = "pc_healthxp", dataType = FloatType, nullable = false),
        StructField(name = "pc_gdp", dataType = FloatType, nullable = false),
        StructField(name = "usd_cap", dataType = FloatType, nullable = false),
        StructField(name = "flag_codes", dataType = StringType, nullable = true),
        StructField(name = "total_spend", dataType = FloatType, nullable = false)
      )
    )

  def row(line: List[String]): Row = Row(line(0), line(1).toInt, line(2).toFloat, line(3).toFloat, line(4).toFloat, line(5), line(6).toFloat)

  val schema = dfSchema(List("country", "time", "pc_healthxp", "pc_gdp", "usd_cap", "flag_codes", "total_spend"))

}
