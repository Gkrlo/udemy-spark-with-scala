import org.apache.log4j._
import org.apache.spark._

/** Find the minimum temperature by weather station */
object DayWithMaxPrcp {
  
  def parseLine(line:String) : (String, Int, String, Float)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val day = fields(1).toInt
    val entryType = fields(2)
    val prcp = fields(3).toFloat
    (stationID, day, entryType, prcp)
  }

  def dayMaxPrcp( value1: (Int, Float), value2 : (Int, Float) ) : (Int, Float) = {

    if(value1._2 >= value2._2){
      value1
    }else
      value2
  }

    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("datasets/1800.csv")
    println("Some datasets lines...")
    lines.take(5).foreach(println)
    println("......................")
    
    // Convert to (stationID, day, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all but PRCP entries
    val maxTemps = parsedLines.filter(x => x._3 == "PRCP" )

    // Convert to (stationID, day, prcp)
    val stationDayPrcp = maxTemps.map(x => (x._1, (x._2.toInt, x._4.toFloat) ))

    // Reduce by stationID retaining the max prcp found
    val dayWithMaxPrcpByStation = stationDayPrcp.reduceByKey( (x,y) => dayMaxPrcp(x,y))

    // Collect, format, and print the results
    dayWithMaxPrcpByStation.collect().foreach(println)

  }
}