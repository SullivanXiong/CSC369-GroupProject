import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object a6 {
  def q1(coursesFileName: String, studentsFileName: String, gradesFileName: String, sc: SparkContext): Unit = {
    val courses = sc.textFile(coursesFileName)
      .map(_.split(","))
      .map(values => (values(0).trim(), values(1).trim().toInt)) // course, difficulty

    val grades = sc.textFile(gradesFileName)
      .map(_.split(","))
      .map(values => (values(1).trim(), values(0).trim().toInt)) // course, student_id

    val students = sc.textFile(studentsFileName)
      .map(_.split(","))
      .map(values => (values(0).trim().toInt, values(1).trim)) // student_id, name

    courses.rightOuterJoin(grades)
      .map(result => (result._2._2, result._2._1)) // student_id, difficulty
      .join(students)
      .map(student => (student._2._1, student._2._2)) // difficulty, student_id
      .groupByKey()
      .max()
      ._2
      .toSet
      .foreach(println)
  }

  def q2(coursesFileName: String, studentsFileName: String, gradesFileName: String, sc: SparkContext): Unit = {
    val coursesTaken = sc.textFile(gradesFileName)
      .map(_.split(","))
      .map(values => (values(1).trim(), values(0).trim().toInt)) // course, student_id

    val students = sc.textFile(studentsFileName)
      .map(_.split(","))
      .map(values => (values(0).trim().toInt, values(1).trim)) // student_id, name

    val courses = sc.textFile(coursesFileName)
      .map(_.split(","))
      .map(values => (values(0).trim(), values(1).trim().toInt)) // course, difficulty

    val studentDifficulties = coursesTaken.join(courses)
      .map(studentCourse => (studentCourse._2._1, studentCourse._2._2))
      .groupByKey()
      .map(student => (student._1, student._2.aggregate((0.0, 0))(
        (acc, v) => (acc._1 + v, acc._2 + 1),
        (acc, pair) => (acc._1 + pair._1, acc._2 + pair._2)
      )))
      .map(student => (student._1, student._2._1 / student._2._2)) // student_id, avg_difficulty

    students.leftOuterJoin(studentDifficulties)
      .foreach(student => println(f"${student._2._1}%s, ${student._2._2.getOrElse(0.0)}%.2f"))
  }

  def q3(coursesFileName: String, sc: SparkContext): Unit = {
    sc.textFile(coursesFileName)
      .map(_.split(","))
      .map(value => (value(1).trim.toInt, value(0).trim))
      .sortByKey(false)
      .take(5)
      .foreach(course => println(course._2))
  }

  def q4(studentsFileName: String, gradesFileName: String, sc: SparkContext): Unit = {
    val grades = sc.textFile(gradesFileName)
      .map(_.split(","))
      .map(values => (values(0).trim().toInt, values(2).trim() match {
        case "A" => 4.0
        case "B" => 3.0
        case "C" => 2.0
        case "D" => 1.0
        case "F" => 0.0
      }))
      .groupByKey()
      .map(student => (student._1, student._2.aggregate((0.0, 0))(
        (acc, v) => (acc._1 + v, acc._2 + 1),
        (acc, pair) => (acc._1 + pair._1, acc._2 + pair._2)
      )))
      .map(student => (student._1, student._2._1 / student._2._2)) // student_id, gpa

    val students = sc.textFile(studentsFileName)
      .map(_.split(","))
      .map(values => (values(0).trim().toInt, values(1).trim)) // student_id, name

    students.leftOuterJoin(grades)
      .map(student => (student._2._2.getOrElse(0.0), student._2._1))
      .sortByKey(false)
      .collect()
      .foreach(student => println(f"${student._2}%s, ${student._1}%.2f"))
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: App [studentsFile] [coursesFile] [gradesFile]")
      return
    }
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("a6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    q1(args(1), args(0), args(2), sc)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    q2(args(1), args(0), args(2), sc)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    q3(args(1), sc)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    q4(args(0), args(2), sc)
  }
}
