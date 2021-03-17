import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// This is a concrete representation of select values from
// a line in a csv file from the COVID-19 dataset
case class CovidReport(country: String,
                       state: String,
                       longitude: Double,
                       latitude: Double,
                       cases: Long,
                       deaths: Long,
                       recovered: Long,
                       active: Long,
                       incidentRate: Double,
                       caseFatalityRatio: Double)

object CovidReport {
  def toDouble(s: String):Option[Double] = {
    try {
      Some(s.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toLong(s: String):Option[Long] = {
    try {
      Some(s.toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def parse(line: String): CovidReport = {
    // This gross regex is necessary to parse the csv properly
    val values = line.split(",(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)")

    // Some values are completely absent from the csv or empty
    // so we have additional logic to handle those cases safely
    CovidReport(
      country = values(3).trim,
      state = values(2).trim,
      latitude = toDouble(values(5).trim).getOrElse(0.0),
      longitude = toDouble(values(6).trim).getOrElse(0.0),
      cases = toLong(values(7).trim).getOrElse(0),
      deaths = toLong(values(8).trim).getOrElse(0),
      recovered = toLong(values(9).trim).getOrElse(0),
      active = toLong(values(10).trim).getOrElse(0),
      incidentRate = if (values.length > 12) {
        toDouble(values(12).trim).getOrElse(0.0)
      } else {
        0.0
      },
      caseFatalityRatio = if (values.length > 13) {
        toDouble(values(13).trim).getOrElse(0.0)
      } else {
        0.0
      }
    )
  }
}

object Project {
  // This is how we calculate the euclidean distance between two feature vectors.
  // It's the standard distance formula applied across corresponding features in both vectors.
  def euclideanDistance(a: Array[Double], b: Array[Double]): Double = {
    Math.sqrt(a.zip(b).map({ case (x, y) => Math.pow(x - y, 2) }).sum)
  }

  // A simple explanation of how K-Nearest Neighbors works can be found at:
  // https://towardsdatascience.com/machine-learning-basics-with-the-k-nearest-neighbors-algorithm-6a6e71d01761
  def knn(data: RDD[(Long, Array[Double])], labels: RDD[(Long, Array[Double])], query: Array[Double], k: Int): Double = {
    val nearestNeighbors = data.map(line => {
      (line._1, euclideanDistance(line._2, query))
    })
      .sortBy(_._2)
      .join(labels)
      .take(k)

    val choice = nearestNeighbors.map(_._2._2(0)).sum / nearestNeighbors.length

    return choice
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: App [vaccinationsFile] [covidFile] [queryFile]")
      return
    }
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("project").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // (lineNumber, CovidReport)
    // We want to be able to join on line number in our knn function
    val covidReports = sc.textFile(args(1))
      .map(CovidReport.parse)
      .zipWithIndex().map(report => (report._2, report._1))

    // Just a single CovidReport selected directly from the csv for testing purposes
    val query = sc.textFile(args(2))
      .map(CovidReport.parse)
      .take(1)(0)

    // Square root of N is often chosen as a good value for K in practice
    //val k = Math.sqrt(covidReports.count()).toInt
    val k = 3

    // We just send latitude and longitude as the data and caseFatalityRatio
    // as the label we want to predict for testing purposes.
    val result = knn(
      covidReports.map(report => (report._1, Array(report._2.latitude, report._2.longitude))),
      covidReports.map(report => (report._1, Array(report._2.caseFatalityRatio))),
      Array(query.latitude, query.longitude),
      k
    )

    println(s"K value: $k")
    println(s"Predicted case fatality ratio: $result")
  }
}
