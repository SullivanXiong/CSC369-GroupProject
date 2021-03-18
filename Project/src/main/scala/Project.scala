import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Location(country: String,
                    population: Long,
                    percentVaccinated: Double)

// This is a concrete representation of select values from
// a line in a csv file from the COVID-19 World Vaccination Progress dataset
case class VaccinationReport(country: String,
                             date: String,
                             percentVaccinated: Double)

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

object VaccinationReport {
  def toDouble(s: String):Option[Double] = {
    try {
      Some(s.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def parse(line: String): VaccinationReport = {
    // This gross regex is necessary to parse the csv properly
    val values = line.split(",(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)")

    // Some values are completely absent from the csv or empty
    // so we have additional logic to handle those cases safely
    VaccinationReport(
      country = values(0).trim,
      date = values(2).trim,
      percentVaccinated = toDouble(values(8).trim).getOrElse(0.0)
    )
  }
}

object project {
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
      .join(labels)
      .sortBy(_._2._1)
      .take(k)

    val choice = nearestNeighbors.map(_._2._2(0)).sum / nearestNeighbors.length

    choice
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("Usage: App [vaccinationsFile] [covidFile] [queryFile] [populationsFile] [queryFile2]")
      return
    }
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("project").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // There are multiple reports per country, one per day.
    // If we want to predict what percentage of the population
    // should be vaccinated from a given population size,
    // we get the most recently reported vaccinated percentage
    // for each country to generate a prediction from.
    // (vaccinationReport)
    val vaccinationReports = sc.textFile(args(0))
      .map(VaccinationReport.parse)
      .groupBy(_.country)
      .map(country => country._2.maxBy(_.percentVaccinated))

    // Retrieve populations per country from file.
    // (country, population)
    val populations = sc.textFile(args(3))
      .map(line => line.split(","))
      .map(line => (line(0).trim, line(1).trim.toLong))

    // Square root of N is often chosen as a good value for K in practice
    //val k = Math.sqrt(populations.count()).toInt
    val k = 3

    // Join populations and vaccinationReports on country
    // and map the results to a Location object containing only what
    // we really care about. Also zipWithIndex to assigned each location
    // a unique id as if we were assigning a lineNumber from a file.
    // (lineNumber, Location)
    val locations = populations.join(vaccinationReports.keyBy(_.country))
      .map(country => Location(country._1, country._2._1, country._2._2.percentVaccinated))
      .zipWithIndex()
      .map(country => (country._2, country._1))

    // The query file should contain a single integer representing the
    // desired population the predict.
    // (population)
    val query = sc.textFile(args(2))
      .map(_.trim.toLong)
      .take(1)(0)

    val prediction = knn(
      locations.map(location => (location._1, Array(location._2.population))),
      locations.map(location => (location._1, Array(location._2.percentVaccinated))),
      Array(query),
      k
    )

    println(s"K value: $k")
    println(s"Predicted percentage of population vaccinated: $prediction\n")

    /* Approximate vaccination percentage of the entire world */
    val population_sum = locations.map(location => location._2.population).sum
    val entire_world_prediction = locations.map(location => location._2.percentVaccinated * (location._2.population.toDouble / population_sum)).sum
    println("Approximate percentage of the entire world vaccinated: %.2f\n".format(entire_world_prediction))

    /* Query2 Case Fatality Rate for given longitude and latitude */
    val covidReports = sc.textFile(args(1))
      .map(CovidReport.parse)
      .zipWithIndex().map(report => (report._2, report._1))

    // A (latitude, longitude) query
    val query2 = sc.textFile(args(4))
      .map(line => line.split(", "))
      .map(line => (line(0).toDouble, line(1).toDouble))
      .take(1)(0)

    // We just send latitude and longitude as the data and caseFatalityRatio
    val result_case_fatality_rate = knn(
      covidReports.map(report => (report._1, Array(report._2.latitude, report._2.longitude))),
      covidReports.map(report => (report._1, Array(report._2.caseFatalityRatio))),
      Array(query2._1, query2._2),
      k
    )

    println(s"Predicted case fatality ratio: $result_case_fatality_rate\n")
  }
}
