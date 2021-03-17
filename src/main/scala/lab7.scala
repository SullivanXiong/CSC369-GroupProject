import org.apache.commons.lang.StringUtils.trim
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

case class Store(ID: Int, state: String, name: String)
case class LineItem(saleID: Int, productID: Int, quantity: Int)
case class Product(ID: Int, price: Float)
case class Sale(ID: Int, storeID: Int, date: String)

object LineItem {
  def parse(line: String): LineItem = {
    val elements = line.split(",").map(trim)
    LineItem(elements(1).toInt, elements(2).toInt, elements(3).toInt)
  }
}

object Product {
  def parse(line: String): Product = {
    val elements = line.split(",").map(trim)
    Product(elements(0).toInt, elements(2).toFloat)
  }
}

object Sale {
  def parse(line: String): Sale = {
    val elements = line.split(",").map(trim)
    Sale(elements(0).toInt, elements(3).toInt, elements(1).substring(0, 7))
  }
}

object Store {
  def parse(line: String): Store = {
    val elements = line.split(",").map(trim)
    Store(elements(0).toInt, elements(5), elements(1))
  }
}

object lab7 {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: App [storesFile] [productsFile] [salesFile] [lineItemsFile]")
      return
    }
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Lab7").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val stores = sc.textFile(args(0)).map(line => Store.parse(line)).keyBy(_.ID)
    val products = sc.textFile(args(1)).map(line => Product.parse(line)).keyBy(_.ID)
    val sales = sc.textFile(args(2)).map(line => Sale.parse(line)).keyBy(_.ID)
    val lineItems = sc.textFile(args(3)).map(line => LineItem.parse(line)).keyBy(_.productID)

    val lineItemTotals = lineItems.join(products)
      .map(result => (result._2._1.saleID, result._2._2.price * result._2._1.quantity))
      .reduceByKey((total, cost) => total + cost) // (sale_id, lineItem_total)

    val monthlyLineItemTotals = lineItemTotals.join(sales)
      .map(result => (result._2._2.date, result._2._2.storeID, result._2._1))
      .groupBy(sale => (sale._1, sale._2)) // (date, store_id), [(date, store_id, lineItem_total)...]

    val storeSaleTotals = monthlyLineItemTotals
      .map(sales => sales._2.reduce({ (total, sale) => (total._1, total._2, total._3 + sale._3) }))
      .map(sale => (sale._2, sale)) // (store_id, (date, store_id, sale_total))

    val monthlyStoreSaleTotals = storeSaleTotals.join(stores)
      .map(result => (result._2._1._1, result._2._2.name, result._2._2.state, result._2._1._3)) // (date, store_name, state, revenue)

    monthlyStoreSaleTotals
      .groupBy(monthlySales => monthlySales._1) // (date, [(date, store_name, state, revenue)...])
      .sortBy(monthlySales => monthlySales._1)
      .map(monthlySales => (monthlySales._1, monthlySales._2
        .toList.sortBy(monthlySales => -monthlySales._4).take(10)))
      .map(monthlySales => s"${monthlySales._1}, " + monthlySales._2
        .map(monthlySales => (monthlySales._2, monthlySales._3, f"$$${monthlySales._4}%.2f")).mkString(", "))
      .saveAsTextFile("lab7.out")
  }
}
