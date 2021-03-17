import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("a6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    println("~~~~~~~~~~~~~~~~~~~~~")

    val readers = sc.parallelize(List(
      (1, "zack", 24),
      (2, "steve", 22),
      (3, "jim", 23),
      (4, "bob", 24)
    ))
    val books = sc.parallelize(List(
      (11, "", 0, "romance"),
      (12, "", 0, "action"),
      (13, "", 0, "historic"),
      (14, "", 0, "historic")
    ))
    val booksRead = sc.parallelize(List(
      (2, 14),
      (1, 11),
      (1, 12),
      (3, 13),
      (3, 14),
      (4, 14),
      (4, 13)
    ))

    val historicalBooks = books.filter(book => book._4 == "historic")
      .map(book => (book._1, book._4))

    val historicalReaders = historicalBooks.leftOuterJoin(booksRead
      .map(bookRead => (bookRead._2, bookRead._1))
    ).map(result => (result._2._2.getOrElse(-1), result._1))
      .groupByKey()
      .filter(keys => keys._2.size == 2)
      .join(readers.keyBy(_._1))

    val youngestReader = historicalReaders
      .sortBy(reader => reader._2._2._3)
      .take(1)(0)._2._2._3

    val youngestHistoricalReaders = historicalReaders.filter(reader => reader._2._2._3 == youngestReader)
      .foreach(println)
    //booksRead.map(bookRead => (bookRead._2, bookRead._1)).join(historicalBooks).foreach(println)
  }
}
