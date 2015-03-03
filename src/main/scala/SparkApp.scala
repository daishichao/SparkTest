/**
 * Created by dsczy on 15-3-3.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SparkApp {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("SparkApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("data/UserPurchaseHistory.csv")
      .map(_.split(","))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

    val numPurchases = data.count()

    println("Total purchases: " + numPurchases)





  }
}
