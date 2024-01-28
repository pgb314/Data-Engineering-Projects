import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.sum
import scala.util.Random

object CSVGenerator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("CSV Generator")
      .master("local[*]") // Set Spark to run in local mode
      .getOrCreate()

    import spark.implicits._

    // Function to generate random date
    def randomDate(startDate: String, endDate: String): String = {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val start = format.parse(startDate).getTime
      val end = format.parse(endDate).getTime
      val date = new java.util.Date(start + (Random.nextDouble() * (end - start)).toLong)
      format.format(date)
    }

    // Generating 100,000 rows of data
    import scala.util.Random

    val data = (1 to 100000).map { _ =>
      (
        randomDate("2020-01-01", "2024-01-01"),  // Random date between 2020 and 2024
        s"Producto${('A' + Random.nextInt(10)).toChar}", // Random product name from productA to productJ
        Random.nextInt(100), // Random quantity
        BigDecimal((15 + Random.nextFloat() * 100).toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat // Random total sale, more than 15, rounded to two decimal places
      )
    }

    // Creating DataFrame
    // Specify the output file path with a specific file name
    val outputPath = "C:\\Users\\Pablo\\Desktop\\Engineering Projects\\Data-Engineering-Projects\\spark\\spark\\ETL\\save.csv"
    val df: DataFrame = data.toDF("Fecha", "Producto", "Cantidad", "VentaTotal")

    println(s"Writing CSV to $outputPath")
    df.coalesce(1).write.option("header", "true").csv(outputPath)
    println("CSV writing complete")


    

    val totalSales = df.agg(sum("VentaTotal").alias("TotalSales")).first().getAs[BigDecimal](0)
    println(s"Total Sales: $totalSales")

    val salesPerProductPerDay = df.groupBy("Fecha", "Producto")
                                  .agg(sum("VentaTotal").alias("VentasProductoPorDia"))

    

    val filteredDF = df.filter($"Producto" === "ProductoA")

    // Show the result (for debugging purposes)

    filteredDF.show()


    val totalSalesFiltered = filteredDF.agg(sum("VentaTotal").alias("VentasTotalesFiltradas")).first().getAs[BigDecimal](0)
    println(s"Total Sales for ProductoA: $totalSalesFiltered")

    val salesPerDayFiltered = filteredDF.groupBy("Fecha")
                                         .agg(sum("VentaTotal").alias("TotalSalesPerDay"))
    
    val cost = 15

    // Creating a new column 'margen' which is VentaTotal - cost
    val dfWithMargen = filteredDF.withColumn("margen", col("VentaTotal") - cost)

    // Show the result with the new column (for debugging purposes)
    dfWithMargen.show()

    val parquetOutputPath = "C:\\Users\\Pablo\\Desktop\\Engineering Projects\\Data-Engineering-Projects\\spark\\spark\\ETL\\ETL_output.parquet"

    // Writing the DataFrame with 'margen' to a Parquet file
    dfWithMargen.write.parquet(parquetOutputPath)

    println(s"DataFrame saved to Parquet at $parquetOutputPath")

   spark.stop()

  }
}
