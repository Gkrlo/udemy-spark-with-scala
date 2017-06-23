import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object RatingCounterDataframes {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = SparkSession.builder().master("local[*]").appName("RatingsCounter").getOrCreate()

    // Load up each line of the ratings data into an RDD
    val lines = sc.read.format("csv").option("delimiter", "\t").load("datasets/ml-100k/u.data").toDF("userID", "movieID", "rating", "timestamp")
    lines.show()

    // Count up how many times each value (rating) occurs
    val results = lines.groupBy("rating").count().sort("count")
    results.show()

    // Print each result on its own line.
    results.collect().foreach(println)

  }

}