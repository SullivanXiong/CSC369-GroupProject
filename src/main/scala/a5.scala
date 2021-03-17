import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object a5 {
  def q1(fileName: String, sc: SparkContext): Unit = {
    println(sc.textFile(fileName)
      .map(_.split(","))
      .flatMap(_.map(_.trim().toInt))
      .filter(_ % 3 == 0)
      .countByValue()
      .map(value => s"${value._1} appears ${value._2} times")
      .mkString(", "))
  }

  def q2(employeesFileName: String, departmentsFileName: String, sc: SparkContext): Unit = {
    val employees = sc.textFile(employeesFileName)
      .map(line => line.split(","))
      .map(values => (values(0).trim, values(1).trim))
    val departments = sc.textFile(departmentsFileName)
      .map(line => line.split(","))
      .map(values => (values(0).trim, values(1).trim))

    employees.cartesian(departments)
      .filter({ case (emp, dep) => emp._2 == dep._1 })
      .map({ case (emp, dep) => s"${emp._1}, ${dep._2}" })
      .foreach(println)
  }

  def q3(fileName: String, sc: SparkContext): Unit = {
    sc.textFile(fileName)
      .map(line => line.split(",", 3))
      .map(values => (values(0).trim, values(1).trim, values(2).split(",")
        .map(course => course.trim.split(" ")(0))
        .map({ case "A" => 4.0 case "B" => 3.0 case "C" => 2.0 case "D" => 1.0 case "F" => 0.0})
        .aggregate((0.0, 0))(
          (acc, gpa) => (acc._1 + gpa, acc._2 + 1),
          (acc, pair) => (acc._1 + pair._1, acc._2 + pair._2)
        )))
      .map(student => f"${student._1}%s, ${student._2}%s, ${student._3._1 / student._3._2}%.2f")
      .foreach(println)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: App [intsFile] [employeesFile] [departmentsFile] [studentsFile]")
      return
    }
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Lab6").setMaster("local[1]")
    val sc = new SparkContext(conf)

    q1(args(0), sc)
    q2(args(1), args(2), sc)
    q3(args(3), sc)
  }
}