import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, LongType, StructType}

object NumberOfPeoplePerMedalByCountries {

  case class TotalMedalsByCountries(country_name: String, num_total: Int, country_id: String)
  case class PopulationByCountries(country_id: String, population: Long)

  def main(args: Array[String]): Unit = {

    // Print only error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create spark session - entry point of the program
    val spark = SparkSession
      .builder()
      .appName("OlympicTokyo2021")
      .master("local[3]")
      .getOrCreate()

    // Schema of the Olympic medal table dataset
    val medalTableSchema = new StructType()
      .add("rank", IntegerType, nullable = true)
      .add("country_name", StringType, nullable = true)
      .add("num_golds", IntegerType, nullable = true)
      .add("num_silvers", IntegerType, nullable = true)
      .add("num_bronzes", IntegerType, nullable = true)
      .add("num_total", IntegerType, nullable = true)
      .add("rank_total", IntegerType, nullable = true)
      .add("country_id", StringType, nullable = true)

    val populationSchema = new StructType()
      .add("country_id", StringType, nullable = true)
      .add("country_name", StringType, nullable = true)
      .add("population", LongType, nullable = true )
      .add("rank", StringType, nullable = true)

    import spark.implicits._
    val medalTableDS = spark.read
      .option("header", "true")
      .schema(medalTableSchema)
      .csv("data/Tokyo_2021_dataset.csv")
      .as[TotalMedalsByCountries]

    val populationDS = spark.read
      .option("header", "true")
      .schema(populationSchema)
      .csv("data/2021_population.csv")
      .as[PopulationByCountries]

    medalTableDS.createOrReplaceTempView("medaltable")
    populationDS.createOrReplaceTempView("population")

    val join_table = spark.sql(
      "select m.country_name, m.num_total, p.population " +
              "from medaltable as m join population as p " +
              "on m.country_id = p.country_id")

    join_table.printSchema()

    val ranking_table = join_table
      .withColumn("number_of_people_per_medal", round(col("population").divide(col("num_total"))))
      .select("country_name", "number_of_people_per_medal")
      .sort("number_of_people_per_medal")

    ranking_table.foreach(f => {
      println(f)
    })

    ranking_table
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("data/statistic_olympic_2021.csv")

    spark.stop()
  }
}
